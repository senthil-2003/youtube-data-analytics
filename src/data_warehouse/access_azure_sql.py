class conn:
    def __init__(self, host_name: str, database_name: str, username: str, password: str, port: str, driver: str = "com.mysql.cj.jdbc.Driver"):
        self.host_name = host_name
        self.database_name = database_name
        self.username = username
        self.password = password
        self.driver = driver
        self.port = port
        
    def get_connection_properties(self) -> dict:
        return {
            "user": self.username,
            "password": self.password,
            "driver": self.driver
        }
        
    def get_jdbc_url(self) -> str:
        return f"jdbc:mysql://{self.host_name}:{self.port}/{self.database_name}?useSSL=true"
