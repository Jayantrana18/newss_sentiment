from sqlalchemy import create_engine

def get_mysql_engine():
    user = "root"
    password = "krish1974"
    host = "localhost"
    port = 3306
    database = "news_sentiment_db"

    engine = create_engine(
        f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
    )
    return engine
