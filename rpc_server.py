from flask import Flask, render_template, request
import pika
import uuid
import threading
import time
from time import sleep
import os

app = Flask(__name__)

class RpcClient:
    internal_lock = threading.Lock()
    queue = {}
    responses = []  # Lista para respuestas

    def __init__(self, rpc_queue):
        self.rpc_queue = rpc_queue
        amqp_url = 'amqps://nvoalptz:Dz-R0OIoGpp3-EhJaA4g8gkxSfk5wzC5@woodpecker.rmq.cloudamqp.com/nvoalptz'
        parameters = pika.URLParameters(amqp_url)  # Usar la URL directamente
        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()
        result = self.channel.queue_declare(queue='', exclusive=True)
        self.callback_queue = result.method.queue
        thread = threading.Thread(target=self._process_data_events)
        thread.daemon = True
        thread.start()

    def _process_data_events(self):
        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self._on_response,
            auto_ack=True
        )
        while True:
            with self.internal_lock:
                self.connection.process_data_events()
                sleep(0.1)

    def _on_response(self, ch, method, props, body):
        respuesta = body.decode()
        self.queue[props.correlation_id] = respuesta
        self.responses.append(respuesta)

    def send_request(self, payload):
        corr_id = str(uuid.uuid4())
        self.queue[corr_id] = None
        with self.internal_lock:
            self.channel.basic_publish(
                exchange='',
                routing_key=self.rpc_queue,
                properties=pika.BasicProperties(
                    reply_to=self.callback_queue,
                    correlation_id=corr_id,
                ),
                body=str(payload)
            )
        return corr_id

rpc_client = RpcClient('rpc_queue')

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        message = request.form['message']
        corr_id = rpc_client.send_request(message)

        timeout = 20
        start_time = time.time()
        while rpc_client.queue[corr_id] is None and (time.time() - start_time) < timeout:
            sleep(0.1)

        if rpc_client.queue[corr_id] is None:
            response = "Error: Timeout esperando respuesta"
        else:
            response = rpc_client.queue[corr_id]

        return render_template('index.html', response=response, all_responses=rpc_client.responses)

    return render_template('index.html', response=None, all_responses=rpc_client.responses)

if __name__ == '__main__':
    # Usa el puerto asignado por Render, por defecto 5000 si no se establece
    port = int(os.getenv('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)
