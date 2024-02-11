import psycopg2
from kafka import KafkaProducer

class CaphcaProducer:
    def __init__(self, host="localhost", port="29092"):
        self.host = host
        self.port = port
        self.producer = KafkaProducer(bootstrap_servers=('%s:%s' % (self.host, self.port)))

    def producer_msg(self, topic_name, key, value):
        self.producer.send(topic=topic_name, key=key, value=value).get(timeout=5)


if __name__ == '__main__':
    employee_topic_name = "kafka_test"
    producer = CaphcaProducer()

    # connect postgres
    conn = psycopg2.connect(
        dbname="postgres",
        user="postgres",
        password="postgres",
        host="localhost",
        port="5432")
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM employees_cdc;")

    rows = cursor.fetchall()
    # df = pd.DataFrame(cdc_records, columns=['emp_id', 'first_name', 'last_name', 'dob', 'city', 'action'])
    for row in range(0,len(rows)):
        producer.producer_msg(topic_name="kafka_test", key=str(rows[row][0]).encode(), value=str(rows[row]).encode())




