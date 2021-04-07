# automated-customer-support
automated-customer-support processes Twitter’s live tweet stream, fetches total coronavirus cases from worldometer.info, merges tweet and coronavirus data, and finally, stores merged data into a MongoDB instance.

## Sequence Diagram
![](https://github.com/pardeepdgr/automated-customer-support/blob/master/Sequence%20Diagram.png)

## How to run from command-line interface
1.) Clone the repository

    git clone https://github.com/pardeepdgr/automated-customer-support.git
    
2.) Change directory to root directory of ```automated-customer-support```
    
    cd automated-customer-support

3.) Install all required dependencies.

    pip3 install -r requirement.txt 
 
4.) Run mongodb container

    docker-compose -f platform/docker-compose.yml up -d
 
5.) Run tweet simulator

    python3 ai_ultimate/simulator/twitter_stream_simulator.py &

6.) Run automated_customer_support

    python3 ai_ultimate/automated_customer_support.py

7.) Now, wait for 20 seconds and check the data in mongodb

    docker exec -it mongodb /bin/bash
    mongo
    show database
    use ultimate_ai
    show collections
    db.messages.find().pretty()
    