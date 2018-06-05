/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dezyre.kafkaproducer.filereader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author m.enudi
 */
public class ClientSimulator {

    private KafkaProducerInterface kafkaProducerInterface;
    private BufferedReader bufferedFileReader;
    private final int pickupTimeIndex;
    private static final DateFormat DATE_FORMATTER = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public ClientSimulator(String file, String brokerUrl, String topic, String fileType) {
        File _file = new File(file);
        if (!_file.exists()) {
            throw new IllegalArgumentException(file + " does not exist");
        }
        try {
            this.bufferedFileReader = new BufferedReader(new FileReader(_file));
        } catch (FileNotFoundException ex) {
            Logger.getLogger(ClientSimulator.class.getName()).log(Level.SEVERE, null, ex);
            throw new RuntimeException(ex.getMessage(), ex);
        }
        //inistantiate kafka endpoint
        this.kafkaProducerInterface = new KafkaProducerInterface(brokerUrl, topic);
        if (fileType.toLowerCase().equals("trip")) {
            this.pickupTimeIndex = 5;
        } else { //fare
            this.pickupTimeIndex = 3;
        }
    }

    public void start() throws Exception {
        int count = 0;
        String line = this.bufferedFileReader.readLine();
        long pickupTs = 0;
        do {
            pickupTs = getPickupTs(line);
            if (pickupTs != 0l) {
                kafkaProducerInterface.send(line, pickupTs);
                count++;
                //flush after a certain number of send message
                if (count == 1000) {
                    kafkaProducerInterface.flush();
                    count = 0;
                    Thread.sleep(10000l); //hold on for 5 secs
                }
                Logger.getLogger(ClientSimulator.class.getName()).log(Level.INFO, "{0}", new Object[]{line});
            }
            line = this.bufferedFileReader.readLine();
        } while (line != null);

        kafkaProducerInterface.flush();
        kafkaProducerInterface.close();
    }

    private long getPickupTs(String line) {
        String pickuDt = line.split(",")[this.pickupTimeIndex];

        try {
            return DATE_FORMATTER.parse(pickuDt).getTime();
        } catch (ParseException ex) {
            Logger.getLogger(ClientSimulator.class.getName()).log(Level.SEVERE, null, ex);
            return 0l;
        }
    }

}
