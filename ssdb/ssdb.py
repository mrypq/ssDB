import gspread
from dateutil import parser
from datetime import datetime
from typing import Any, TypeAlias, Iterator
from typing_extensions import Self
from dataclasses import dataclass, InitVar, field, asdict
from gspread import Worksheet, Spreadsheet
from gspread.utils import ValueRenderOption, ValueInputOption

from .utils import Yaml


Cell: TypeAlias = Any | str | int | float
Record: TypeAlias = list[dict[Any, Cell]]
dtfmt = '%Y-%m-%d %H:%M:%S'


class Connector:
    @staticmethod
    def connect(book_id: str) -> Spreadsheet:
        gc = gspread.oauth()
        return gc.open_by_key(book_id)


@dataclass(slots=True, kw_only=True)
class Scheme:
    # DB Column
    created_at: str = field(default_factory=lambda: datetime.now().strftime(dtfmt))
    updated_at: str = field(default_factory=lambda: datetime.now().strftime(dtfmt))
    _primary_key = 'primary_key' # attribute name to use as primary_key

    def set_updated_at(self):
        self.updated_at = datetime.now().strftime(dtfmt)

    @property
    def created_time(self) -> datetime:
        return parser.parse(self.created_at)

    @property
    def updated_time(self) -> datetime:
        return parser.parse(self.updated_at)

    @property
    def primary_key_value(self) -> Any:
        return getattr(self, self._primary_key)

    def aslist(self, header: list[str]=[]) -> list[Any]:
        if header:
            return [getattr(self, h) for h in header]
        else:
            return list(asdict(self).values())

    @classmethod
    def parse(cls, data: dict[str, Any]) -> Self:
        args = {}
        for k, v in data.items():
            if k in cls.__slots__:
                args.update({k: v})
        return cls(**args)

@dataclass(slots=True)
class Table:
    name: str
    scheme: type[Scheme]
    book: InitVar[Spreadsheet]
    ws: Worksheet = field(init=False)

    def __post_init__(self, book: Spreadsheet):
        self.ws = book.worksheet(self.name)

    @property
    def records(self) -> Iterator[dict[str, Cell]]:
        records = self.ws.get_all_records(
            head=1,
            value_render_option=ValueRenderOption.unformatted,
        )
        yield from records

    @property
    def schemes(self) -> Iterator[Scheme]:
        for record in self.records:
            yield self.scheme(**record)
    
    @property
    def header(self) -> list[str]:
        return self.ws.row_values(
            1,
            value_render_option=ValueRenderOption.unformatted,
        )


    def get(self, primary_key: Cell) -> Scheme|None:
        for r in self.schemes:
            if r.primary_key_value == primary_key:
                return r
        return None

    def gets(self, **query) -> Iterator[Scheme]:
        for r in self.schemes:
            if not self.check_query(r, **query):
                continue
            yield r

    def overwrite(self, data: list[Scheme]):
        header = self.header
        rows = [d.aslist(header) for d in data]
        self.ws.clear()
        rows.insert(0, header)
        self.ws.update('A1', rows)

    def appends(self, data: list[Scheme]):
        header = self.header
        rows = [d.aslist(header) for d in data]
        self.ws.append_rows(
            values=rows,
            value_input_option=ValueInputOption.user_entered,
            table_range='A1',
        )

    def yaml_dump(self, path: str):
        data = {self.name: list(self.records)}
        Yaml.dump(path, data)

    @staticmethod
    def check_query(record: Scheme, **query) -> bool:
        for k, v in query.items():
            match v:
                case tuple()|list()|set():
                    if getattr(record, k) not in v:
                        return False
                case _:
                    if getattr(record, k) != v:
                        return False
        return True