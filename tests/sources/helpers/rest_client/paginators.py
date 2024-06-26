class BasePaginator:
    def __init__(self, records):
        self.records = records

    @property
    def page_records(self):
        """Return records for the current page."""
        raise NotImplementedError

    @property
    def metadata(self):
        """Return metadata for the current page.
        E.g. total number of records, current page number, etc.
        """
        raise NotImplementedError

    @property
    def next_page_url_params(self):
        """Return URL parameters for the next page.
        This is used to generate the URL for the next page in the response.
        """
        raise NotImplementedError


class PageNumberPaginator(BasePaginator):
    def __init__(self, records, page_number, page_size=5, index_base=1):
        """Paginate records by page number.

        Args:
            records: List of records to paginate.
            page_number: Page number to return.
            page_size: Maximum number of records to return per page.
            index_base: Index of the start page. E.g. zero-based
                index or 1-based index.
        """
        super().__init__(records)
        self.page_number = page_number
        self.index_base = index_base
        self.page_size = page_size

    @property
    def page_records(self):
        start_index = (self.page_number - self.index_base) * self.page_size
        end_index = start_index + self.page_size
        return self.records[start_index:end_index]

    @property
    def metadata(self):
        return {"page": self.page_number, "total_pages": self.total_pages}

    @property
    def next_page_url_params(self):
        return {"page": self.next_page_number} if self.next_page_number else {}

    @property
    def total_pages(self):
        total_records = len(self.records)
        return (total_records + self.page_size - 1) // self.page_size

    @property
    def next_page_number(self):
        return (
            self.page_number + 1
            if self.page_number + 1 < self.total_pages + self.index_base
            else None
        )


class OffsetPaginator(BasePaginator):
    def __init__(self, records, offset, limit=10):
        """Paginate records by offset.

        Args:
            records: List of records to paginate.
            offset: Offset to start slicing from.
            limit: Maximum number of records to return.
        """
        super().__init__(records)
        self.offset = offset
        self.limit = limit

    @property
    def page_records(self):
        return self.records[self.offset : self.offset + self.limit]

    @property
    def metadata(self):
        return {"total_records": len(self.records), "offset": self.offset, "limit": self.limit}

    @property
    def next_page_url_params(self):
        if self.offset + self.limit < len(self.records):
            return {"offset": self.offset + self.limit, "limit": self.limit}
        return {}


class CursorPaginator(BasePaginator):
    def __init__(self, records, cursor, limit=5):
        """Paginate records by cursor.

        Here, cursor is the index of the record to start slicing from.

        Args:
            records: List of records to paginate.
            cursor: Cursor to start slicing from.
            limit: Maximum number of records to return.
        """
        super().__init__(records)
        self.cursor = cursor
        self.limit = limit

    @property
    def page_records(self):
        return self.records[self.cursor : self.cursor + self.limit]

    @property
    def metadata(self):
        next_index = self.cursor + self.limit

        if next_index < len(self.records):
            next_cursor = next_index
        else:
            next_cursor = None

        return {"next_cursor": next_cursor}
