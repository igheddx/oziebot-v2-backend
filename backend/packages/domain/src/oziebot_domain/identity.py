from enum import StrEnum
from uuid import UUID

UserId = UUID


class Role(StrEnum):
    ROOT_ADMIN = "root_admin"
    USER = "user"
