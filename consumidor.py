import pika
import time
import sys

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

def connect():
    """Establece la conexión con RabbitMQ y configura el canal"""
    amqp_url = 'amqps://nvoalptz:Dz-R0OIoGpp3-EhJaA4g8gkxSfk5wzC5@woodpecker.rmq.cloudamqp.com/nvoalptz'
    parameters = pika.URLParameters(amqp_url)

    connection = None
    channel = None

    while connection is None or not connection.is_open:
        try:
            print("Intentando conectar a RabbitMQ...")
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()
            channel.queue_declare(queue='rpc_queue')
            # Asegúrese de no recibir más de un mensaje a la vez
            channel.basic_qos(prefetch_count=1)
            print("Conexión exitosa a RabbitMQ.")
        except pika.exceptions.AMQPConnectionError as e:
            print(f"Error de conexión: {e}")
            print("Esperando 5 segundos antes de intentar nuevamente...")
            time.sleep(5)
    
    return connection, channel

def main():
    """Consumidor principal"""
    connection, channel = connect()

    def reconfigurar_conexion():
        """Función para manejar la reconexión"""
        print("Reconectando...")
        connection.close()
        channel.close()
        connection, channel = connect()
        return connection, channel

    try:
        # Iniciar el consumo de mensajes
        channel.basic_consume(
            queue='rpc_queue',
            on_message_callback=on_request
        )

        print(" [*] Esperando mensajes RPC. Presiona CTRL+C para salir")
        channel.start_consuming()

    except pika.exceptions.AMQPConnectionError as e:
        print(f"Error en la conexión: {e}. Intentando reconectar...")
        connection, channel = reconfigurar_conexion()
        # Volver a intentar el consumo después de la reconexión
        channel.basic_consume(queue='rpc_queue', on_message_callback=on_request)
        channel.start_consuming()

if __name__ == '__main__':
    main()
