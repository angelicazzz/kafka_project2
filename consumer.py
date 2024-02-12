from confluent_kafka import Consumer, KafkaError
import psycopg2
import json
import ast

class KafkaPostgresConsumer:
    def __init__(self, kafka_bootstrap_servers, kafka_topic, db_name, db_user, db_password, db_host='localhost', db_port='5438'):
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.kafka_topic = kafka_topic
        self.db_name = db_name
        self.db_user = db_user
        self.db_password = db_password
        self.db_host = db_host
        self.db_port = db_port

    def consume_and_store(self):
        # Kafka consumer configuration
        conf = {
            'bootstrap.servers': self.kafka_bootstrap_servers,
            'group.id': 'my_consumer_group',
            'auto.offset.reset': 'earliest'
        }

        consumer = Consumer(conf)
        consumer.subscribe([self.kafka_topic])

        # PostgreSQL connection
        conn = psycopg2.connect(
            dbname=self.db_name,
            user=self.db_user,
            password=self.db_password,
            host=self.db_host,
            port=self.db_port
        )
        cur = conn.cursor()

        try:
            while True:
                msg = consumer.poll(1.0)
                print("<<<<<msg:",msg)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        print(msg.error())
                        break

                # Parse the message (Assuming message is a tuple in string representation)
                data = ast.literal_eval(msg.value().decode('utf-8'))
                print("Kafka data here <<<<<<<<<<<<<<<<<<<<<<<<<<<", data)

                if data[5] == 'INSERT':
                    cur.execute("""
                        INSERT INTO employees (emp_id, first_name, last_name, dob, city) 
                        VALUES (%s, %s, %s, %s, %s)
                    """, (data[0], data[1], data[2], data[3], data[4]))
                elif data[5] == 'UPDATE':
                    cur.execute("""
                        UPDATE employees 
                        SET first_name = %s, last_name = %s, dob = %s, city = %s
                        WHERE emp_id = %s
                    """, (data[1], data[2], data[3], data[4],data[0]))
                elif data[5] == 'DELETE':
                    cur.execute("""
                        DELETE FROM employees WHERE emp_id = %s
                    """, (data[0],))
                conn.commit()

                
        except KeyboardInterrupt:
            pass

        finally:
            consumer.close()
            cur.close()
            conn.close()

if __name__ == '__main__':
    consumer = KafkaPostgresConsumer(
        kafka_bootstrap_servers='localhost:29092',
        kafka_topic='kafka_test',
        db_name='postgres',
        db_user='postgres',
        db_password='postgres'
    )
    consumer.consume_and_store()
