#!/usr/bin/env python
# coding: utf-8

def load_env_variables():
    """Load environment variables from .env file and return them as a dictionary."""
    load_dotenv()
    env_vars = {
        "PG_HOST": os.getenv("PG_HOST"),
        "PG_PORT": os.getenv("PG_PORT"),
        "PG_USER": os.getenv("PG_USER"),
        "PG_PASSWORD": os.getenv("PG_PASSWORD"),
        "PG_DATABASE": os.getenv("PG_DATABASE"),
        "KAFKA_TOPIC1": os.getenv("KAFKA_TOPIC1"),
        "KAFKA_BOOTSTRAP_SERVER": ['localhost:9092']
    }
    return env_vars

def connect_to_kafka(topic, bootstrap_servers):
    """Create and return a KafkaConsumer instance."""
    return KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='earliest'
    )

def connect_to_postgresql(host, port, user, password, database):
    """Create and return a psycopg2 connection and cursor."""
    conn = psycopg2.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database
    )
    cur = conn.cursor()
    return conn, cur

def insert_data_to_postgresql(cur, conn, data):
    """Insert data into PostgreSQL table."""
    insert_query = """
    INSERT INTO metrics (id, cpu, mem, disk)
    VALUES (%s, %s, %s, %s)
    """
    numbers = tuple([int(item.split(":")[1].strip()) for item in data.split(",")])
    cur.execute(insert_query, numbers)
    conn.commit()
    print(f"Received message: value = {numbers}")

def consume_messages(consumer, cur, conn):
    """Consume messages from Kafka and insert into PostgreSQL."""
    for message in consumer:
        data = message.value.decode('utf-8')
        insert_data_to_postgresql(cur, conn, data)

def main():
    """Main function to orchestrate the Kafka to PostgreSQL data pipeline."""
    env_vars = load_env_variables()

    consumer = connect_to_kafka(env_vars["KAFKA_TOPIC1"], env_vars["KAFKA_BOOTSTRAP_SERVER"])
    conn, cur = connect_to_postgresql(env_vars["PG_HOST"], env_vars["PG_PORT"], env_vars["PG_USER"], env_vars["PG_PASSWORD"], env_vars["PG_DATABASE"])

    try:
        consume_messages(consumer, cur, conn)
    except Exception as e:
        print(f"Error: {e}")
    finally:
        consumer.close()
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()