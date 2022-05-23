var Web3 = require("web3");
var AWS = require("aws-sdk");


// AWS Region.
const REGION = "eu-west-2"; 
const NODE_ENDPOINT = ""
const SNS_TOPIC_ARN = ""
// Web3 connection
const web3 = new Web3(NODE_ENDPOINT);

let options = {
    fromBlock: "latest",
    address: [],    //Only get events from specific addresses
    topics: []      //What topics to subscribe to
};

// Initiate subscription
let subscription = web3.eth.subscribe('logs', options,(err,event) => {
    if (!err)
    console.log(event)
});


function pushToSNS(message){

    // Params for pushing message
    var params = {
        Message: JSON.stringify(message), // MESSAGE_TEXT
        TopicArn: SNS_TOPIC_ARN, //TOPIC_ARN
      };
    // Create promise and SNS service object
    var publishTextPromise = new AWS.SNS({region: REGION}).publish(params).promise();

    // Handle promise's fulfilled/rejected states
    publishTextPromise.then(
    function(data) {
        console.log(`Message ${params.Message} sent to the topic ${params.TopicArn}`);
        console.log("MessageID is " + data.MessageId);
    }).catch(
        function(err) {
        console.error("AWS Error:", err, err.stack);
    });

}

subscription.on('data', event => console.log(event))
subscription.on('data', event => pushToSNS(event))
subscription.on('changed', changed => console.log(changed))
subscription.on('error', err => { throw err })
subscription.on('connected', nr => console.log('Subscribed succeccsfully! ', nr))