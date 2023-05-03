# Configurar la ruta de login:
# https://pypi.org/project/fastapi-login/
from fastapi import Depends, APIRouter
from fastapi.security import OAuth2PasswordRequestForm
from fastapi_login.exceptions import InvalidCredentialsException
from ..dependencies import load_user, manager


router = APIRouter()


@router.post('/auth/token')
def login(data: OAuth2PasswordRequestForm = Depends()):
    email = data.username
    password = data.password

    user = load_user(email)
    if email != user['login']:
        raise InvalidCredentialsException
    elif password != user['password']:
        raise InvalidCredentialsException

    # 15 minutes default expiration:
    access_token = manager.create_access_token(
        data=dict(sub=email)
    )
    # Set token duration:
    # expires after 12 hours
    # from datetime import timedelta
    # access_token = manager.create_access_token(
    #     data=dict(sub=email), expires=timedelta(hours=12)
    # )
    return {'access_token': access_token, 'token_type': 'bearer'}
