import pika
import time

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

def main():
    # Conexión a CloudAMQP usando la URL proporcionada
    amqp_url = 'amqps://nvoalptz:Dz-R0OIoGpp3-EhJaA4g8gkxSfk5wzC5@woodpecker.rmq.cloudamqp.com/nvoalptz'
    
    # Configuración de la conexión
    parameters = pika.URLParameters(amqp_url)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    
    # Declarar la cola
    channel.queue_declare(queue='rpc_queue')
    
    # Asegurarse de no recibir más de un mensaje a la vez
    channel.basic_qos(prefetch_count=1)
    
    # Iniciar el consumo de mensajes
    channel.basic_consume(
        queue='rpc_queue',
        on_message_callback=on_request
    )
    
    print(" [*] Esperando mensajes RPC. Presiona CTRL+C para salir")
    channel.start_consuming()

if __name__ == '__main__':
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
