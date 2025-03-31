from typing import Type

from dlt.destinations.impl.sqlalchemy.factory import sqlalchemy
class starrocks(sqlalchemy):
    
    @property
    def client_class(self) -> Type["StarrocksJobClient"]:
        from dlt.destinations.impl.starrocks.job_client import StarrocksJobClient
        return StarrocksJobClient


starrocks.register()
