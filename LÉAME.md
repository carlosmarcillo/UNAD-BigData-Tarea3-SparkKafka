# Tarea 3: Procesamiento de Datos con Apache Spark y Kafka
**Curso:** Big Data (Código: 202016911)  
**Universidad:** Universidad Nacional Abierta y a Distancia (UNAD)  
**Estudiante:** [Tu Nombre Completo]  

## Descripción del Proyecto
Este repositorio contiene los desarrollos prácticos para el análisis de datos en **Batch** (PySpark) y en **Tiempo Real** (Spark Streaming + Apache Kafka) siguiendo las orientaciones de la Guía 3 y el Anexo 3.

## Estructura del Repositorio
- `kafka_producer.py`: Script en Python que genera datos simulados de telemetría y los envía al topic de Kafka.
- `spark_streaming_consumer.py`: Script de PySpark Streaming que lee el flujo de Kafka y procesa métricas en tiempo real.
- `batch_processing.py`: Script de procesamiento en batch con PySpark DataFrames.

## Instrucciones de Ejecución
1. **Iniciar servicios de Kafka:**
   ```bash
   sudo /opt/Kafka/bin/zookeeper-server-start.sh /opt/Kafka/config/zookeeper.properties &
   sudo /opt/Kafka/bin/kafka-server-start.sh /opt/Kafka/config/server.properties &
