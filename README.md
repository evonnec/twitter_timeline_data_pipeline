# Senior Data Pipeline Engineer - Code Test

## Objective

Design and implement a serverless data pipeline using Python and AWS services. The pipeline should extract data from the Twitter API, process the data, and store the results in an appropriate storage solution.

## Compensation

This exercise pays $1,000 USD via ACH, Check, or Wire upon completion. You will receive an invitation enabling you to provide your preferred payment details directly to [Mercury.com](http://Mercury.com).

## Timeline

We ask that the exercise be completed and returned within 7 days.

## Requirements

1. Use the provided access token to authenticate and interact with the Twitter API, and collect the following data:
    - Timeline tweets from a specific Twitter user
    - Metadata (e.g., `created_at`, `text`, `referenced_tweets`, etc) for each Tweet
    - Media (e.g., images, video, etc) attached to each Tweet
2. Implement a simple serverless architecture using AWS Lambda, AWS SQS, AWS S3, and AWS DynamoDB. The data pipeline should perform the following tasks:
    - Trigger a Lambda function to call the Twitter API and retrieve the data.
    - Send the retrieved data to an SQS queue for asynchronous processing.
    - Create another Lambda function to process the data from the SQS queue. This function should:
        - Extract the Tweet metadata and media.
        - Store the Tweet metadata in an Amazon DynamoDB table.
        - Store the media in an Amazon S3 bucket.
3. Optimize the data pipeline for efficiency, reliability, and scalability.
4. Demonstrate best practices related to secrets management.
5. Document your solution, including the serverless architecture, data models, and any assumptions made during the implementation.

You final solution should at least have the following artifacts:

- A CloudFormation template written in YAML that can be used to deploy resources required by your app, including:
    - Amazon DynamoDB table for storing Tweet metadata
    - Amazon S3 bucket for storing media related to a Tweet (attached images, etc)
    - Amazon SQS queue for enqueueing Tweets for asynchronous downstream processing
- Separate Python scripts for each of the required Lambda functions
    - Lambda for retrieving data from the Twitter API and sending it to the SQS queue (triggered by a scheduled CloudWatch event)
    - Lambda for processing messages from the SQS queue (triggered by a new message in the SQS queue)
- A README file containing detailed instructions on how to deploy and test the data pipeline, as well as any assumptions made during the implementation.

You may also *optionally* use a serverless framework for completing this exercise. Nuclei regularly uses [AWS Chalice](https://aws.github.io/chalice/index.html), which is lightweight and makes it very easy to build and [quickly deploy](https://aws.github.io/chalice/quickstart.html) serverless applications. Familiarity with the AWS Chalice framework is not required for this exercise, and you are free to:

- Use a different serverless framework of your choice.
- Deploy your Lambda functions using your CloudFormation template.
- Deploy your Lambda functions using the AWS CLI.
- Deploy your Lambda functions using the AWS Console.
- Deploy your Lambda functions using a different tool of your choice.

The goal of this exercise is to assess your ability to implement a simple serverless data pipeline using Python and AWS services. You are free to use any Python libraries or AWS services that you feel are appropriate for the task.

## Evaluation Criteria

Your solution will be evaluated based on the following criteria:

- Functionality: The data pipeline should successfully extract, process, and store the data as per the requirements.
- Serverless architecture: The solution should effectively leverage AWS serverless services (Lambda, SQS, S3, and DynamoDB).
- Code quality: The Python code should be clear, well-organized, and maintainable.
- Documentation: The documentation should be thorough and easy to understand, allowing others to deploy and test the data pipeline without any issues.
- Optimization: The data pipeline should be optimized for efficiency, reliability, and scalability.

## Additional Resources

### Amazon Web Services

You can sign up for a free AWS account here that can be used to deploy and test your application:

[https://aws.amazon.com/free](https://aws.amazon.com/free)

### Twitter API v2

#### Authorization

You will need to use the following Bearer token to communicate with the Twitter API v2:

```
AAAAAAAAAAAAAAAAAAAAACMbnAEAAAAAy%2FYCSnrBg1sa3hM8rq8nGOMyTa4%3DCEd5n1KOX6ITr37gmjPWDAoiIqXufr4eqkdObzO06E5WGg2ukh
```

This is a throwaway, rate-limited token that provides read-only access to already public data.

To check if the token is working from a bash terminal:

```bash
curl -H "Authorization: Bearer $ACCESS_TOKEN" https://api.twitter.com/2/users/by/username/NucleiAI
```

Example: successful response:

```bash
{"data":{"id":"1075929753503363077","name":"Nuclei","username":"NucleiAI"}}
```

Example: unsuccessful response (malformed or expired token):

```bash
{
  "title": "Unauthorized",
  "type": "about:blank",
  "status": 401,
  "detail": "Unauthorized"
}
```

Upon completion of the test Nuclei will expire the token to prevent accidental re-use.

#### Documentation

The Twitter API v2 is documented [here](https://developer.twitter.com/en/docs). 

You will need to make use of the Timelines [endpoints](https://developer.twitter.com/en/docs/twitter-api/tweets/timelines/introduction) to retrieve Tweets from a specific Twitter user.

#### Scope

For the purposes of the test, you should retrieve data from a well-known, public domain, frequently updated Twitter account. Accounts that might meet this criteria would include the following:

1. [NASA](https://twitter.com/NASA)
2. [National Geographic](https://twitter.com/natgeo)
3. [TED Talks](https://twitter.com/tedtalks)
4. [World Health Organization (WHO)](https://twitter.com/who)

## Submission

You should use this GitHub repository for submitting your code test. Please ensure you commit the following:

- Your code.
- Any necessary configuration files required for the implementation.
- A README file containing detailed instructions on how to deploy and test the data pipeline, as well as any assumptions made during the implementation.

## Support

It’s OK to ask for help if you feel stuck or if something isn’t working!

Good luck and happy coding! 🚀
