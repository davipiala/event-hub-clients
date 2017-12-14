var Kafka = require('no-kafka');

var connString = ' kafka://129.144.160.38:6667, 129.144.160.38:6667 '
var consumer = new Kafka.SimpleConsumer({ connectionString: connString });
 
var dataHandler = function (messageSet, topic, partition) {
    messageSet.forEach(function (m) {
        console.log('topic received: ');
        console.log({
            'topic':topic,
            'partition': partition,
            'offset': m.offset,
            'message': m.message.value.toString('utf8')
        });
    });
};
 
return consumer.init()
.then(function () {
    return consumer.subscribe('myfirsteventhub', 0, dataHandler);
});