# Tweet Mood

Analyze tweets to find their sentiment and extract entities.


## App workflow

1. A `Local` instance is initiated. It checks whether there is a `Manager` instance already running in EC2, and if not, starts one.
2. Uploads a file with links to tweets to S3.
3. Establishes SQS queues to exchange messages with `Manager`, and uploads their URLs in a file to S3.
4. `Manager` downloads the files and connects to these queues.
5. `Manager` establishes queues to communicate with `Worker` instances - they will actually download the tweets, parse them, run the analysis, produce and send a result message.
6. `Manager` starts various threads that:
    1. Await incoming requests from `Local` instances. When one is detected, starts a new thread that decomposes the request to small jobs.
    2. Await incoming results from `Worker` instances.
    3. Await for requests that are finished, and thus can compile the results into a summary file to be uploaded to S3.
    4. See if `Worker` instances have unexpectedly shutdown, and raise replacement instances.
7. If a `Local` instance is executed with the `terminate` flag, will commence a graceful shutdown of the `Worker` instances and itself, provided that all the requests have been dealt with.
8. The local machine that runs a `Local` instance will have an `HTML` file with the output. The S3 bucket will have a summary file that describes the labor of the `Worker` instances.

Note: all EC2 instances are bootstrapped using a shell script (`user data` startup script in EC2 jargon).

## Dependencies

1. [Java 8](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)
2. External JARs
    1. [JSoup](https://jsoup.org/)
    2. [ejml 0.23](http://repo1.maven.org/maven2/com/googlecode/efficient-java-matrix-library/ejml/0.23/ejml-0.23.jar)
    3. [Stanford CoreNLP 3.3.0](http://repo1.maven.org/maven2/edu/stanford/nlp/stanford-corenlp/3.3.0/stanford-corenlp-3.3.0.jar) and [models 3.3.0](http://repo1.maven.org/maven2/edu/stanford/nlp/stanford-corenlp/3.3.0/stanford-corenlp-3.3.0-models.jar)
    4. [jollyday 0.4.7](http://jollyday.sourceforge.net/)
    5. [AWS SDK for Java](https://aws.amazon.com/sdk-for-java/)

## What you need to change to make this work on your computer

1. The AWS S3 bucket name in `Manager.java` and `Local.java`.
2. The AWS EC2 AMI number in `Manager.java` and `Local.java`. Make sure this AMI has Java 8 installed and has been updated with `sudo yum update`.
3. Specifically define the `IAM role` for use in `Manager.java` and `Local.java`. Make sure this role has the following policies: S3 full access, EC2 full access, SQS full access, Administrator privileges.
3. Zip the project in 2 JARs: `Manager.jar` and `Worker.jar`. Upload these JARs and the rest from the previous section to the S3 bucket you're using.


## Launching the app
1. Make sure all the JARs referenced in `Local.java` are properly linked in your IDE project. Alternatively, if you're running from a terminal, have them all in project folder.
2. To run from a terminal (if using an IDE, configure the command line arguments accordingly):

    `java <input filename> <HTML output filename> <n> [terminate]`
    
    where - 
        `n` - the number of tweets per Worker. This is used in `Manager` to determine how many EC2 instances should be raised as `Worker`s
        `[terminate]` - if this local client should send a termination message to the `Manager`, once the results file has been downloaded.


## License

The MIT License (MIT)

Copyright (c) 2016 Asaf Chelouche, Ben Ben-Chaya

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

