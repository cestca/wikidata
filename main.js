const SOURCE = 'D:\\wikidata\\latest-all.json\\latest-all.json'
const IS_CLUSTER = true

const cluster = IS_CLUSTER ? require( 'cluster' ) : null
const read = require( './reader.js' )
const factory = require( './factory.js' )

let TASK = process.argv[2]

if( TASK == null || TASK.trim().length == 0 ){
    console.error( 'bad input:' , TASK )
    process.exit(1)
}

let task = null

try{
    task = require( './tasks/' + TASK + '.js' )

} catch( error ){
    console.error( 'task not found:' , TASK )
    process.exit(2)
}

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

if( ! IS_CLUSTER || cluster.isMaster ){

    if( task.start ){ task.start() }

    if( IS_CLUSTER ){
        factory.hire( cluster , task )
    }

    read( SOURCE , ( line , readSpeed ) => {

        if( IS_CLUSTER ){
            factory.adjust( cluster , task , readSpeed )
            factory.pick().send( line )

        } else {
            work( line , ( message ) => { task.handleMessage( message ) } )
        }

    } ).then( () => {

        if( task.end ){ task.end() }

        factory.workers.forEach( w => {
            w.send( { method: 'exit' } )
        })
    })

} else {

    process.on( 'message' , ( message ) => {

        if( message.method && message.method && message.method == 'exit' ){

            process.exit( 0 )
        
        } else {

            work( message , (reply) => {
                process.send(reply)
            } )
        }
    } )
}