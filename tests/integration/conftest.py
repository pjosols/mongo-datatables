import pytest
from datetime import datetime
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure


@pytest.fixture(scope="session")
def mongo_db():
    try:
        client = MongoClient("localhost", 27017, serverSelectionTimeoutMS=2000)
        client.admin.command("ping")
    except ConnectionFailure:
        pytest.skip("MongoDB not available")
    db = client["mongo_datatables_test"]
    yield db
    client.drop_database("mongo_datatables_test")
    client.close()


@pytest.fixture
def books_col(mongo_db):
    col = mongo_db["books"]
    col.drop()
    col.insert_many([
        {"Title": "1984", "Author": "George Orwell", "Pages": 328,
         "PublisherInfo": {"Date": datetime(1949, 6, 8)}, "Genre": "Dystopia"},
        {"Title": "Brave New World", "Author": "Aldous Huxley", "Pages": 311,
         "PublisherInfo": {"Date": datetime(1932, 1, 1)}, "Genre": "Dystopia"},
        {"Title": "Fahrenheit 451", "Author": "Ray Bradbury", "Pages": 158,
         "PublisherInfo": {"Date": datetime(1953, 10, 19)}, "Genre": "Dystopia"},
        {"Title": "The Great Gatsby", "Author": "F. Scott Fitzgerald", "Pages": 180,
         "PublisherInfo": {"Date": datetime(1925, 4, 10)}, "Genre": "Fiction"},
        {"Title": "To Kill a Mockingbird", "Author": "Harper Lee", "Pages": 281,
         "PublisherInfo": {"Date": datetime(1960, 7, 11)}, "Genre": "Fiction"},
        {"Title": "Of Mice and Men", "Author": "John Steinbeck", "Pages": 112,
         "PublisherInfo": {"Date": datetime(1937, 2, 6)}, "Genre": "Fiction"},
        {"Title": "The Catcher in the Rye", "Author": "J.D. Salinger", "Pages": 277,
         "PublisherInfo": {"Date": datetime(1951, 7, 16)}, "Genre": "Fiction"},
        {"Title": "Animal Farm", "Author": "George Orwell", "Pages": 112,
         "PublisherInfo": {"Date": datetime(1945, 8, 17)}, "Genre": "Satire"},
        {"Title": "Lord of the Flies", "Author": "William Golding", "Pages": 224,
         "PublisherInfo": {"Date": datetime(1954, 9, 17)}, "Genre": "Fiction"},
        {"Title": "Slaughterhouse-Five", "Author": "Kurt Vonnegut", "Pages": 215,
         "PublisherInfo": {"Date": datetime(1969, 3, 31)}, "Genre": "Satire"},
    ])
    col.create_index([("Title", "text"), ("Author", "text")])
    yield col
    col.drop()


def make_request(draw=1, start=0, length=10, search_value="", columns=None, order=None, **extra):
    """Build a minimal DataTables request dict."""
    cols = columns or [
        {"data": "Title", "searchable": "true", "orderable": "true", "search": {"value": ""}},
        {"data": "Author", "searchable": "true", "orderable": "true", "search": {"value": ""}},
        {"data": "Pages", "searchable": "true", "orderable": "true", "search": {"value": ""}},
        {"data": "Genre", "searchable": "true", "orderable": "true", "search": {"value": ""}},
    ]
    req = {
        "draw": draw,
        "start": start,
        "length": length,
        "search": {"value": search_value, "regex": False, "smart": True, "caseInsensitive": True},
        "columns": cols,
        "order": order or [{"column": 0, "dir": "asc"}],
    }
    req.update(extra)
    return req
