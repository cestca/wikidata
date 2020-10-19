const SOURCE = 'D:\\wikidata\\latest-all.json\\latest-all.json'
const IS_CLUSTER = true
const CLUSTER_WORKERS = 2

const cluster = IS_CLUSTER ? require( 'cluster' ) : null
const tasks = require( './tasks.js' )
const read = require( './reader.js' )

let TASK = process.argv[2]

if( TASK == null || TASK.trim().length == 0 || ! Object.keys(tasks).includes(TASK) ){
    console.error( 'bad input' , TASK )
    process.exit(1)
}

let work = ( taskName , line , reply ) => {

    if( line != '[' && line != ']' ){

        let json = {}

        try{
            json = JSON.parse( line[line.length-1] == ',' ? line.slice(0,line.length-1) : line )
        }catch( e ){
            console.error( 'JSON PARSE ERROR' , e.message )
        }

        try{
            tasks[ taskName ].step( json , reply )
        }catch( e ){
            console.error( 'ERROR IN TASK' , e.message )
        }
    }

}

if( ! IS_CLUSTER || cluster.isMaster ){

    let task = tasks[TASK]
    let workers = []

    if( IS_CLUSTER ){
        while( workers.length < CLUSTER_WORKERS ){
            let worker = cluster.fork( { TASK } )
            worker.on( 'message' , (message) => { task.handleMessage(message) } )
            worker.on( 'online' , () => { console.error( 'WORKER IS ONLINE' , worker.id ) }  )
            worker.on( 'listening' , () => { console.error( 'WORKER LSTENING' , worker.id ) } )
            worker.on( 'disconnect' , () => { console.error( 'WORKER DISCONNECTED' , worker.id ) } )
            worker.on( 'exit' , () => { console.error( 'WORKER EXIT' , worker.id ) } )
            workers.push( worker )
        }
    }

    let lineCount = 0

    read( SOURCE , task , ( line ) => {

        lineCount += 1

        if( IS_CLUSTER ){
            workers[ lineCount % workers.length ].send( line )

        } else {
            work( TASK , line , ( message ) => { task.handleMessage( message ) } )

        }

    } )




} else {

    process.on( 'message' , ( message ) => {
        work( TASK , message , (reply) => {
            process.send(reply)
        } )
    } )

}

