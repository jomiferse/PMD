from sqlalchemy.orm import Session

from ..models import User, UserAlertPreference
from .effective_settings import EffectiveSettings, get_effective_settings_for_user

EffectiveUserSettings = EffectiveSettings


def get_effective_user_settings(
    user: User,
    pref: UserAlertPreference | None = None,
    db: Session | None = None,
) -> EffectiveSettings:
    return get_effective_settings_for_user(user, pref=pref, db=db)
