package org.example;

import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.sqs.model.Message;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;


import static org.example.WorkerService.MANAGER_REQUEST_QUEUE;
import static org.example.WorkerService.MANAGER_TO_WORKER_REQUEST_QUEUE;

public class ManagerApp {

    public static final String LOCAL_TO_MANAGER_REQUEST_QUEUE = "LocalToManagerRequestQueue";
    public static final String MANAGER_TO_LOCAL_REQUEST_QUEUE = "ManagerToLocalRequestQueue";
    public static final String WORKER_TO_MANAGER_REQUEST_QUEUE = "WorkerToManagerRequestQueue";

    public static void run(String[] args){
        if (args.length != 3){
            Logger.getLogger().log("Invalid arguments. Usage: <accessKeyId> <secretAccessKey> <sessionToken>");
            Logger.getLogger().log("got args: " + String.join(", ", args));
            return;
        }
        try {
            WorkerService workerService = WorkerService.getInstance(args[0], args[1], args[2]);
//            workerService.startWorkers()
            Instance workerInstance = workerService.getSingleWorker();
        }
        catch (RuntimeException e){
            SqsService.sendMessage(MANAGER_TO_LOCAL_REQUEST_QUEUE, "I Am the manager and i could not setup worker instance. error is: " + e.getMessage()+ " Exiting...");
            System.out.println("Manager setup failed.with error: " + e.getMessage() + " Exiting...");
            return;
        }

        new Thread(()->{
            while (ExpectingMoreMessagesFromLocalApps()){
                //get messages from workers, name of queue is misleading, will fix later
                List<Message> messages = SqsService.getMessagesForQueue(LOCAL_TO_MANAGER_REQUEST_QUEUE);
                if (!messages.isEmpty()){
                    for (Message message : messages) {
                        Logger.getLogger().log("Received message: " + message.body());
                        handleLocalAppMessage(message);
                        SqsService.deleteMessage(LOCAL_TO_MANAGER_REQUEST_QUEUE, message);
                    }
                }
                else SqsService.sendMessage(MANAGER_TO_LOCAL_REQUEST_QUEUE, "no messages from local apps at this time.");

            }
            postProccess();
        })
                .start();

        while (ExpectingMoreMessagesFromWorkers()){
            //get messages from workers, name of queue is misleading, will fix later
            List<Message> messages = SqsService.getMessagesForQueue(WORKER_TO_MANAGER_REQUEST_QUEUE);
            if (!messages.isEmpty()){
                for (Message message : messages) {
                    Logger.getLogger().log("Received message: " + message.body());
                    handleWorkerMessage(message);
                    SqsService.deleteMessage(WORKER_TO_MANAGER_REQUEST_QUEUE, message);
                }
            }
            else SqsService.sendMessage(MANAGER_TO_LOCAL_REQUEST_QUEUE, "I Am the manager and i have no messages from workers at this time.");

        }
        postProccess();

    }

    //temp var, remove later
    static int expectedNumberOfMessages = 10;
    private static boolean ExpectingMoreMessagesFromWorkers() {
        //for now, always return true,
        //suggested implementation: have a list of returned files from workers, when reached expected number, return false.
        //manage that list using the handleWorkerMessage method.
        if (expectedNumberOfMessages > 0){
            expectedNumberOfMessages--;
            return true;
        }
        return false;
    }

    static void handleWorkerMessage(Message message){
        Logger.getLogger().log("Handling worker message: " + message);
        //for now, just log the message to the local-to-manager queue
        SqsService.sendMessage(MANAGER_TO_LOCAL_REQUEST_QUEUE, "I Am the manager and here is a message forwarded from a worker: " + message.body());

    }
    static void postProccess(){
        Logger.getLogger().log("Manager post processing started.");
        //suggested implementation: terminate all workers, clean up SQS queues, send final message to local client.
        SqsService.sendMessage(MANAGER_TO_LOCAL_REQUEST_QUEUE, "I Am the manager and i have finished all my work. Exiting...");
        Logger.getLogger().log("Manager post processing finished.");
    }

    private static boolean ExpectingMoreMessagesFromLocalApps() {
        //for now, always return true,
        //suggested implementation: have a list of returned files from local apps, when reached expected number, return false.
        //manage that list using the handleWorkerMessage method.
        return true;
    }

    static void handleLocalAppMessage(Message message){
        Logger.getLogger().log("Handling local app message: " + message);
        File file = S3Service.downloadFile(message.body());
        //parse lines
        try {
            BufferedReader reader = new BufferedReader(new java.io.FileReader(file));
            String line;
            while ((line = reader.readLine()) != null) {
                Logger.getLogger().log("Read line: " + line);
                //send each line to a worker
                SqsService.sendMessage(MANAGER_TO_WORKER_REQUEST_QUEUE, line);
            }
        } catch (IOException e) {
            Logger.getLogger().log("Error reading file: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }
}
