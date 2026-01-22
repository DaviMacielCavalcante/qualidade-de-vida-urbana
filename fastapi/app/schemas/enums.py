from enum import Enum 

class UserRoleEnum(str, Enum):
    admin = "admin"
    user = "user"

class UserNotificationPreferenceEnum(str, Enum):
    yes = "yes"
    no = "no"

class UserSignatureStatusEnum(str, Enum):
    active = "active"
    inactive = "inactive"

class SubscriptionStatusEnum(str, Enum):
    active = "active"
    inactive = "inactive"
    cancelled = "cancelled"
    expired = "expired"
    pending = "pending"
    suspended = "suspended"
    refunded = "refunded"
    failed = "failed"
    disputed = "disputed"