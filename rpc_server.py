from flask import Flask
import pika
import time
import threading
import os

app = Flask(__name__)

# Función para procesar los mensajes de RabbitMQ
def procesar_mensaje(mensaje):
    """Simula procesamiento complejo"""
    print(f"Procesando: {mensaje}")
    time.sleep(2)  # Simula trabajo
    return f"PROCESADO: {mensaje.upper()}"

def on_request(ch, method, props, body):
    mensaje = body.decode()
    print(f" [x] Recibido: {mensaje}")
    
    response = procesar_mensaje(mensaje)
    
    ch.basic_publish(
        exchange='',
        routing_key=props.reply_to,
        properties=pika.BasicProperties(
            correlation_id=props.correlation_id
        ),
        body=response
    )
    ch.basic_ack(delivery_tag=method.delivery_tag)
    print(f" [✓] Enviada respuesta para: {mensaje}")

def iniciar_consumidor():
    """Función para iniciar el consumidor de RabbitMQ"""
    amqp_url = 'amqps://nvoalptz:Dz-R0OIoGpp3-EhJaA4g8gkxSfk5wzC5@woodpecker.rmq.cloudamqp.com/nvoalptz'
    parameters = pika.URLParameters(amqp_url)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.queue_declare(queue='rpc_queue')
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='rpc_queue', on_message_callback=on_request)
    print(" [*] Esperando mensajes RPC. Presiona CTRL+C para salir")
    channel.start_consuming()

@app.route('/')
def index():
    return "Servidor Flask en ejecución"

if __name__ == '__main__':
    # Arrancar el servidor Flask en un hilo separado
    port = int(os.getenv('PORT', 5000))
    threading.Thread(target=iniciar_consumidor, daemon=True).start()  # Inicia el consumidor en segundo plano
    app.run(host='0.0.0.0', port=port)
