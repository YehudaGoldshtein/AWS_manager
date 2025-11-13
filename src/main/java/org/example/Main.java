package org.example;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.QueueDoesNotExistException;

public class Main {
    public static void main(String[] args) {
        System.out.println("Hello world!");
        start();
    }

    public static String queueName = "MyQueue";
    public static void start(){
        // first we check if manager already exists by checking if sqs queue exists
        SqsClient sqsClient = SqsClient.builder()
                .region(Region.US_EAST_1) // Example region, change as needed
                .build();

        try {
            GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder()
                    .queueName(queueName)
                    .build();

            sqsClient.getQueueUrl(getQueueUrlRequest);
            System.out.println("Queue exists. Manager is already running.");
            // If getQueueUrl succeeds, the queue exists
        } catch (QueueDoesNotExistException e) {
            System.out.println("Queue does not exist. Proceeding to create EC2 instance for manager.");
        }

        }
}