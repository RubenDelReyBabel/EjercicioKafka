package com.helloworld.ejercicio;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.Callback;

import com.helloworld.ejercicio.model.Camion;

import io.confluent.kafka.serializers.KafkaAvroSerializer;

public class CamionProducer {
	
    public static void main(final String[] args){
        // Configuración del productor
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:29092");
        props.put("key.serializer", KafkaAvroSerializer.class);
        props.put("value.serializer", KafkaAvroSerializer.class);
        props.put("schema.registry.url", "http://localhost:8085");
        
        String keySchemaString = readFileFromResources("kilometraje.key.avsc");
        String valueSchemaString = readFileFromResources("kilometraje.value.avsc");

        Schema keySchema = new Schema.Parser().parse(keySchemaString);
        Schema valueSchema = new Schema.Parser().parse(valueSchemaString);

        final String topic = "kilometraje";
        final int repeticiones = 10;

        List<Camion> camiones = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            camiones.add(new Camion(String.valueOf(i), String.valueOf(i)));
        }

        final Producer<GenericRecord, GenericRecord> producer = new KafkaProducer<>(props);
            
        for (int i = 0; i < repeticiones; i++) {
            try{
                Thread.sleep(10000);
                for (Camion camion : camiones) {
                    camion.avanzar();
                    GenericRecord keyRecord = new GenericData.Record(keySchema);
                    keyRecord.put("key", camion.getId());

                    GenericRecord valueRecord = new GenericData.Record(valueSchema);
                    valueRecord.put("matricula", camion.getMatricula());
                    valueRecord.put("km", camion.getKm());
                    valueRecord.put("velocidad", camion.getVelocidad());
                    valueRecord.put("timestamp", camion.getTimestamp());
                    
                    producer.send(
                        new ProducerRecord<>(topic, keyRecord, valueRecord)
                        ,new ProducerCallback());
                }
            }
            catch(InterruptedException e){
                e.printStackTrace();
            }
        }
        
        producer.close();

    }

    public static String readFileFromResources(String fileName) {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        try (InputStream inputStream = classLoader.getResourceAsStream(fileName);
             BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
            return reader.lines().collect(Collectors.joining("\n"));
        } catch (IOException e) {
            throw new RuntimeException("Error al leer el archivo " + fileName + " desde src/main/resources", e);
        }
    }
}

class ProducerCallback implements Callback {
	@Override
	public void onCompletion(RecordMetadata metadata, Exception exception) {
		if (exception == null) {
			System.out.printf("Produced event to topic %s offset= %d partition=%d%n", 
					metadata.topic(), metadata.offset(), metadata.partition());
		} else {
			exception.printStackTrace();
		}
		
	}
}

