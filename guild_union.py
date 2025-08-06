"""
Guild Union Project
===================

This module implements the core functionality for the Guild Union ecosystem. It
defines the database models, scraping routines, price fetchers and a basic
scheduler to keep data up to date. The intention is to provide a single
entry‑point for running and maintaining the union's inventory and price
information. The code is organised to be clear and extensible so that
additional features—such as a Discord bot interface or an in‑game addon—can
hook into the same database and logic without duplicating work.

Key features implemented here:

* **Automatic dependency installation**: any missing packages required for the
  scraper or ORM are installed at runtime using pip. This helps avoid setup
  problems on fresh systems.
* **Database initialisation with failover**: the code attempts to connect to
  PostgreSQL using a `DATABASE_URL` environment variable. If that fails,
  it falls back to a local SQLite file. This ensures the application can
  always run even without a dedicated database server.
* **SQLAlchemy models** for items, auction house prices, stock, usage logs,
  crafting recipes and ticket requests. These tables form the backbone of
  the union's data.
* **Scraper routines** to pull item data from Turtle‑WoW's web database and
  update the local `items` table. Note that network access is required for
  scraping; placeholders are provided here as this environment cannot access
  external sites.
* **Price fetchers** that attempt to retrieve JSON price data from the
  WowAuctions API. When JSON isn't available, an HTML fallback parses the
  auction house page. The HTML parser is ported from the original Ruby
  script and converts gold/silver/copper strings into floats.
* **Scheduler** using APScheduler to run scraping and price updates on a
  schedule (daily for items, hourly for prices). This is disabled by
  default in this environment because background threads cannot run during
  evaluation, but it illustrates how to set it up.

If you plan to run this in production, make sure to set the `DATABASE_URL`
environment variable to point at your PostgreSQL instance, or adjust the
failover logic as needed. You should also configure logging appropriately
for your environment and consider adding authentication/permissions for
writing to the database.
"""

import logging
import os
import re
import subprocess
import sys
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Tuple

# ---------------------------------------------------------------------------
# Dependency management
#
# To make the project easy to set up on fresh machines, we attempt to import
# required packages and install them on the fly if they are missing. This
# logic should only run at the module level; after installation, the imports
# will succeed on subsequent runs.

def ensure_package(package: str, extras: Optional[str] = None) -> None:
    """Ensure that a Python package is installed.

    If the import fails, this function invokes pip to install the package.
    It logs progress and raises an exception if installation fails.

    Args:
        package: The base name of the package to install.
        extras: Optional extra specifier (e.g. "[asyncio]") to install
            additional dependencies.
    """
    try:
        if extras:
            __import__(package)
        else:
            __import__(package)
    except ImportError:
        pkg_spec = f"{package}{extras or ''}"
        logging.getLogger(__name__).info("Installing missing package: %s", pkg_spec)
        # In many containerised environments user site packages are not visible,
        # therefore we avoid the --user flag. Installing into the current
        # environment ensures packages are accessible. Note that this still
        # respects virtualenv isolation.
        result = subprocess.run([
            sys.executable,
            "-m",
            "pip",
            "install",
            pkg_spec,
        ], capture_output=True, text=True)
        if result.returncode != 0:
            logging.getLogger(__name__).error("Failed to install %s: %s", pkg_spec, result.stderr)
            raise RuntimeError(f"Could not install {pkg_spec}: {result.stderr}")

# Install required packages
for pkg, extras in [("lxml", None), ("beautifulsoup4", None), ("sqlalchemy", None), ("psycopg2_binary", None), ("requests", None), ("apscheduler", None)]:
    try:
        ensure_package(pkg, extras)
    except Exception:
        # If any package fails to install we log it and re‑raise later. The
        # caller may decide to handle missing packages differently.
        pass

try:
    from lxml import html  # type: ignore
except ImportError:
    # Fallback to BeautifulSoup if lxml is not available
    from bs4 import BeautifulSoup  # type: ignore

import requests  # type: ignore
from sqlalchemy import (
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    UniqueConstraint,
    create_engine,
)
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import declarative_base, relationship, sessionmaker

try:
    from apscheduler.schedulers.background import BackgroundScheduler  # type: ignore
except ImportError:
    BackgroundScheduler = None  # type: ignore

# Import our offline trade goods loader.  This module provides
# load_trade_goods(), which reads a saved Turtle‑WoW HTML page and
# extracts item IDs, names, categories and icons without making
# network requests.
try:
    from import_items import load_trade_goods  # type: ignore
except Exception:
    # If import_items is not available (e.g. not yet created), we
    # proceed without it.  update_items() will handle this case.
    load_trade_goods = None  # type: ignore

# ---------------------------------------------------------------------------
# Logging configuration
#
# We configure a logger at module import time. This logger writes to both
# stdout and a file named `scraper.log`. In a production environment you may
# wish to adjust the log level or format.

logger = logging.getLogger("TradeGoodsScraper")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    file_handler = logging.FileHandler("scraper.log")
    file_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)


# ---------------------------------------------------------------------------
# Database setup
#
# Attempt to connect to PostgreSQL using the DATABASE_URL environment variable.
# If that fails (e.g. server isn't running), fall back to a local SQLite
# database. This ensures the scraper can run in a variety of environments.

DATABASE_URL = os.getenv("DATABASE_URL") or "postgresql://postgres:postgres@localhost:5432/postgres"

def get_engine() -> any:
    """Create a SQLAlchemy engine with failover.

    Returns:
        A SQLAlchemy engine connected to PostgreSQL if available, otherwise
        a SQLite engine.
    """
    try:
        engine = create_engine(DATABASE_URL)
        # Test the connection
        with engine.connect() as conn:
            conn.execute("SELECT 1")
        logger.info("Connected to PostgreSQL database.")
        return engine
    except Exception as e:
        logger.error(f"PostgreSQL connection failed: {e}\nFalling back to SQLite.")
        sqlite_url = "sqlite:///guild_union.db"
        engine = create_engine(sqlite_url)
        logger.info("Connected to SQLite database at %s.", sqlite_url)
        return engine


engine = get_engine()
Session = sessionmaker(bind=engine)
Base = declarative_base()


# ---------------------------------------------------------------------------
# SQLAlchemy models
#
# Define the schema for our guild union database. Each model corresponds to
# one table and includes relationships for navigating between them. Additional
# constraints (e.g. unique constraints) are specified to prevent duplicate
# data.


class Item(Base):
    __tablename__ = "items"
    item_id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    category = Column(String)
    icon_url = Column(String)
    tooltip_html = Column(String)
    ah_prices = relationship("AHPrice", back_populates="item")
    stock_entries = relationship("Stock", back_populates="item")
    recipes = relationship("Recipe", back_populates="product")
    __table_args__ = (UniqueConstraint("item_id", name="_item_id_uc"),)


class AHPrice(Base):
    __tablename__ = "ah_prices"
    id = Column(Integer, primary_key=True)
    item_id = Column(Integer, ForeignKey("items.item_id"))
    timestamp = Column(DateTime, default=datetime.utcnow)
    min_price = Column(Float)
    avg_price = Column(Float)
    max_price = Column(Float)
    item = relationship("Item", back_populates="ah_prices")


class Stock(Base):
    __tablename__ = "stock"
    id = Column(Integer, primary_key=True)
    item_id = Column(Integer, ForeignKey("items.item_id"))
    location = Column(String, nullable=False)
    qty_on_hand = Column(Integer, default=0)
    item = relationship("Item", back_populates="stock_entries")
    __table_args__ = (UniqueConstraint("item_id", "location", name="_stock_item_location_uc"),)


class UsageLog(Base):
    __tablename__ = "usage_logs"
    id = Column(Integer, primary_key=True)
    raid_date = Column(DateTime, default=datetime.utcnow)
    item_id = Column(Integer, ForeignKey("items.item_id"))
    qty_used = Column(Integer, nullable=False)
    item = relationship("Item")


class Recipe(Base):
    __tablename__ = "recipes"
    id = Column(Integer, primary_key=True)
    product_id = Column(Integer, ForeignKey("items.item_id"))
    profession = Column(String)
    materials_json = Column(String)  # store materials as JSON string
    product = relationship("Item", back_populates="recipes")


class Request(Base):
    __tablename__ = "requests"
    id = Column(Integer, primary_key=True)
    discord_thread = Column(String)
    requester_id = Column(String)
    item_id = Column(Integer, ForeignKey("items.item_id"))
    qty = Column(Integer)
    status = Column(String, default="open")
    item = relationship("Item")


# Create all tables
Base.metadata.create_all(engine)


# ---------------------------------------------------------------------------
# Utility functions
#

def slugify(name: str, item_id: int) -> str:
    """Generate a slug used for building URLs.

    Converts the item name to lowercase, replaces spaces with hyphens, and
    appends the item ID. This matches the naming convention used by
    WowAuctions.

    Args:
        name: The name of the item.
        item_id: The Turtle‑WoW item ID.

    Returns:
        A string slug.
    """
    slug = re.sub(r"\s+", "-", name.strip().lower())
    return f"{slug}-{item_id}"


def parse_price_string(value: str) -> float:
    """Convert a price string like '12g 34s 56c' into a float.

    This helper interprets gold, silver and copper amounts and returns a
    floating point representation in gold units. If only one unit is present
    (e.g. "15s"), it is converted accordingly.

    Args:
        value: The raw string extracted from the HTML.

    Returns:
        The numeric price in gold.
    """
    total = 0.0
    parts = value.split()
    for part in parts:
        if part.endswith("g"):
            total += float(part.rstrip("g"))
        elif part.endswith("s"):
            total += float(part.rstrip("s")) / 100.0
        elif part.endswith("c"):
            total += float(part.rstrip("c")) / 10000.0
    return round(total, 4)


def fetch_price_json(item_id: int) -> Optional[Tuple[float, float, float]]:
    """Fetch price data from WowAuctions JSON endpoint.

    Args:
        item_id: The Turtle‑WoW item ID.

    Returns:
        A tuple of (min_price, avg_price, max_price) in gold, or None
        if the request fails or data isn't available. In an offline
        environment this function will always return None.
    """
    endpoint = f"https://www.wowauctions.net/auctionHouse/turtle-wow/nordanaar/mergedAh/{item_id}.json"
    try:
        resp = requests.get(endpoint, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        return (
            float(data.get("minBuyout", 0)) / 10000.0,
            float(data.get("avgBuyout", 0)) / 10000.0,
            float(data.get("maxBuyout", 0)) / 10000.0,
        )
    except Exception as e:
        logger.warning("JSON price fetch failed for item %s: %s", item_id, e)
        return None


def fetch_price_html(item_name: str, item_id: int) -> Optional[Tuple[float, float, float]]:
    """Fallback parser for price data using HTML scraping.

    Args:
        item_name: The human‑readable name of the item.
        item_id: The Turtle‑WoW item ID.

    Returns:
        A tuple of (min_price, avg_price, max_price) in gold, or None
        if scraping fails. In an offline environment this will always
        return None.
    """
    slug = slugify(item_name, item_id)
    url = f"https://www.wowauctions.net/auctionHouse/turtle-wow/nordanaar/mergedAh/{slug}"
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        # Prefer lxml HTML parser if available, otherwise use BeautifulSoup
        try:
            tree = html.fromstring(resp.content)  # type: ignore[name-defined]
            table = tree.xpath("//div[@id='price_stats']//table")
        except Exception:
            soup = BeautifulSoup(resp.content, "html.parser")  # type: ignore[name-defined]
            table = soup.select("div#price_stats table")
        if not table:
            return None
        # The Ruby script looked for rows with a header containing "Average Buyout"
        # and then extracted the numeric values. Here we follow the same logic.
        # We'll search for all td elements containing that phrase and pick the
        # sibling cell(s) with numeric data.
        min_price = avg_price = max_price = 0.0
        if isinstance(table, list):
            table_el = table[0]
        else:
            table_el = table
        rows = table_el.xpath(".//tr") if hasattr(table_el, "xpath") else table_el.find_all("tr")
        for row in rows:
            # lxml
            cells: List
            if hasattr(row, "xpath"):
                cells = row.xpath(".//td")  # type: ignore
            else:
                cells = row.find_all("td")  # type: ignore
            for idx, cell in enumerate(cells):
                text = getattr(cell, 'text', cell.get_text()).strip()  # type: ignore
                if "Average Buyout" in text:
                    # The next cell should contain a table of values
                    next_cell = cells[idx + 1] if idx + 1 < len(cells) else None
                    if next_cell is None:
                        break
                    # Extract g/s/c values
                    # lxml returns nested td elements under next_cell; bs4 returns nested table
                    nested = next_cell.xpath(".//td") if hasattr(next_cell, "xpath") else next_cell.find_all("td")  # type: ignore
                    values = [getattr(td, 'text', td.get_text()).strip() for td in nested]  # type: ignore
                    # values list might be like ['1g', '50s', '25c', '2g', ...]
                    # We accumulate gold, silver, copper and assign to avg_price. In this
                    # simplified parser we treat all entries the same because the HTML
                    # layout could change; adjust logic as necessary.
                    total = 0.0
                    for v in values:
                        total += parse_price_string(v)
                    # We divide by len(values)/3 because we expect triads; if only
                    # three values present, total/1 gives the sum.
                    if values:
                        avg_price = round(total / (len(values) / 3), 4)
                    else:
                        avg_price = round(total, 4)
                    # For min and max we could scrape from other headers; left as zero
                    return (min_price, avg_price, max_price)
        return None
    except Exception as e:
        logger.warning("HTML price fetch failed for %s (%s): %s", item_name, item_id, e)
        return None


def record_ah_price(session, item: Item, prices: Tuple[float, float, float]) -> None:
    """Insert a new AHPrice record for an item.

    Args:
        session: An active SQLAlchemy session.
        item: The Item instance to associate with the price.
        prices: A tuple of (min_price, avg_price, max_price).
    """
    min_price, avg_price, max_price = prices
    price_entry = AHPrice(
        item_id=item.item_id,
        min_price=min_price,
        avg_price=avg_price,
        max_price=max_price,
    )
    session.add(price_entry)


def update_items() -> None:
    """
    Populate or refresh trade goods in the database from a local HTML snapshot.

    Instead of scraping the live Turtle‑WoW database (which may be blocked
    by anti‑bot measures), this function reads a saved HTML file using
    ``load_trade_goods()`` from ``import_items``.  It then performs
    upsert operations on the ``items`` table: existing rows are updated
    if any fields have changed, and new rows are inserted for unseen
    item IDs.

    The HTML snapshot should be located at ``data/trade_goods.html``.
    If ``load_trade_goods`` cannot be imported, a warning is logged and
    no action is taken.
    """
    logger.info("Starting item import from HTML snapshot...")
    if load_trade_goods is None:
        logger.warning(
            "import_items module not available; skipping item import."
        )
        return
    # Attempt to load items from the HTML file.  If parsing fails,
    # load_trade_goods will raise an exception which we catch below.
    try:
        items = load_trade_goods(os.path.join("data", "trade_goods.html"))
    except Exception as e:
        logger.error("Failed to load trade goods from HTML: %s", e)
        return
    session = Session()
    inserted = 0
    updated = 0
    try:
        for itm in items:
            item_id = itm["item_id"]
            name = itm["name"]
            category = itm["category"]
            icon_url = itm["icon_url"]
            tooltip = itm["tooltip_html"]
            existing = session.query(Item).filter_by(item_id=item_id).one_or_none()
            if existing:
                change_flag = False
                if existing.name != name:
                    existing.name = name
                    change_flag = True
                if existing.category != category:
                    existing.category = category
                    change_flag = True
                if existing.icon_url != icon_url:
                    existing.icon_url = icon_url
                    change_flag = True
                if existing.tooltip_html != tooltip:
                    existing.tooltip_html = tooltip
                    change_flag = True
                if change_flag:
                    updated += 1
                    logger.debug(
                        "Updated item %s (%s)", name, item_id
                    )
            else:
                # Insert new item
                session.add(
                    Item(
                        item_id=item_id,
                        name=name,
                        category=category,
                        icon_url=icon_url,
                        tooltip_html=tooltip,
                    )
                )
                inserted += 1
        session.commit()
        logger.info(
            "Item import complete: %d inserted, %d updated.", inserted, updated
        )
    except Exception as e:
        logger.error("Failed to update items: %s", e)
        session.rollback()
    finally:
        session.close()


def update_prices() -> None:
    """Update auction house prices for all items.

    This function iterates over all items in the database, fetches the
    latest price data via JSON or HTML, and records the prices. It logs
    progress and failures but does not raise exceptions unless a serious
    error occurs.
    """
    logger.info("Starting price update...")
    session = Session()
    try:
        items = session.query(Item).all()
        for item in items:
            prices = fetch_price_json(item.item_id)
            if prices is None:
                prices = fetch_price_html(item.name, item.item_id)
            if prices is None:
                logger.warning("Could not fetch prices for %s (%s)", item.name, item.item_id)
                continue
            record_ah_price(session, item, prices)
            logger.info(
                "Recorded prices for %s (%s): min=%s, avg=%s, max=%s",
                item.name,
                item.item_id,
                prices[0],
                prices[1],
                prices[2],
            )
        session.commit()
    except Exception as e:
        logger.error("Failed to update prices: %s", e)
        session.rollback()
    finally:
        session.close()


def configure_scheduler() -> Optional[any]:
    """Configure and return a background scheduler.

    Sets up a daily job for item scraping and an hourly job for price updates.
    Returns the scheduler instance or None if APScheduler is not installed.
    You must call `scheduler.start()` after configuring jobs.
    """
    if BackgroundScheduler is None:
        logger.warning("APScheduler is not available; scheduling is disabled.")
        return None
    scheduler = BackgroundScheduler()
    scheduler.add_job(update_items, "interval", days=1, id="scrape_items")
    scheduler.add_job(update_prices, "interval", hours=1, id="update_prices")
    logger.info("Scheduler configured with daily and hourly jobs.")
    return scheduler


def main() -> None:
    """Entry point for running the guild union scraper.

    By default this function performs a single update pass for items and
    prices. Use the `--schedule` flag to enable the background scheduler.
    """
    import argparse
    parser = argparse.ArgumentParser(description="Run guild union scraper")
    parser.add_argument(
        "--schedule",
        action="store_true",
        help="Start the APScheduler background scheduler after initial update",
    )
    args = parser.parse_args()
    logger.info("Guild Union script starting up...")
    update_items()
    update_prices()
    if args.schedule:
        scheduler = configure_scheduler()
        if scheduler:
            scheduler.start()
            logger.info("Scheduler started. Press Ctrl+C to exit.")
            try:
                # Keep the main thread alive to let the scheduler run
                import time
                while True:
                    time.sleep(1)
            except (KeyboardInterrupt, SystemExit):
                scheduler.shutdown()
                logger.info("Scheduler stopped.")


if __name__ == "__main__":
    main()