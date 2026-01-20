from apollo.egress.agent.service.file_login_token_provider import FileLoginTokenProvider

_SECRET_STRING_PATH = "/usr/local/creds/secret_string"


class SNALoginTokenProvider(FileLoginTokenProvider):
    def __init__(self):
        super().__init__(_SECRET_STRING_PATH)
