const SOURCE = 'D:\\wikidata\\latest-all.json\\latest-all.json'
const IS_CLUSTER = true
let CLUSTER_WORKERS = 2

const cluster = IS_CLUSTER ? require( 'cluster' ) : null
const tasks = require( './tasks.js' )
const read = require( './reader.js' )
const { kill } = require('process')

let TASK = process.argv[2]

if( TASK == null || TASK.trim().length == 0 || ! Object.keys(tasks).includes(TASK) ){
    console.error( 'bad input' , TASK )
    process.exit(1)
}

let workers = []
let task = tasks[ TASK ]

const work = ( line , reply ) => {

    if( line != '[' && line != ']' ){

        let json = {}

        try{
            json = JSON.parse( line[line.length-1] == ',' ? line.slice(0,line.length-1) : line )
        }catch( e ){
            console.error( 'JSON PARSE ERROR' , e.message )
        }

        try{
            task.step( json , reply )
        }catch( e ){
            console.error( 'ERROR IN TASK' , e.message )
        }
    }

}

const createWorker = () => {

    let worker = cluster.fork()
    worker.on( 'message' , (message) => { task.handleMessage(message) } )
    worker.on( 'online' , () => { console.error( 'WORKER IS ONLINE' , worker.id ) }  )
    worker.on( 'listening' , () => { console.error( 'WORKER LSTENING' , worker.id ) } )
    worker.on( 'disconnect' , () => { console.error( 'WORKER DISCONNECTED' , worker.id ) } )
    worker.on( 'exit' , () => { console.error( 'WORKER EXIT' , worker.id ) } )
    workers.push( worker )
    CLUSTER_WORKERS = workers.length

}

const removeWorker = () => {
    workers.pop()
    CLUSTER_WORKERS = workers.length
}

const adjustCluster = ( workerCount ) => {

    while( workers.length < workerCount ){
        createWorker()
    }

    while( workers.length > workerCount ){
        removeWorker()
    }

}

if( ! IS_CLUSTER || cluster.isMaster ){

    if( IS_CLUSTER ){
        adjustCluster( CLUSTER_WORKERS )
    }

    let lineCount = 0
    let lastReadSpeeds = []

    read( SOURCE , task , ( line , readSpeed ) => {

        lineCount += 1

        if( IS_CLUSTER ){

            if( readSpeed > 0 ){
                lastReadSpeeds[ CLUSTER_WORKERS ] = readSpeed
            }

            if( lastReadSpeeds[ CLUSTER_WORKERS + 1 ] == null ){

            }

            workers[ lineCount % workers.length ].send( line )

        } else {
            work( line , ( message ) => { task.handleMessage( message ) } )

        }

    } )




} else {

    process.on( 'message' , ( message ) => {
        work( message , (reply) => {
            process.send(reply)
        } )
    } )

}

