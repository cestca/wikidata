const SOURCE='D:\\wikidata\\latest-all.json\\latest-all.json'

const cluster = require( 'cluster' )
const tasks = require( './tasks.js' )
const filter = require( './filter.js' )

if( cluster.isMaster ){

    let TASK = process.argv[2]

    if( TASK == null || TASK.trim().length == 0 || ! Object.keys(tasks).includes(TASK) ){
        console.error( 'bad input' , TASK )
        process.exit(1)
    }

    let task = tasks[TASK]

    let workers = [
        cluster.fork( { TASK } ),
        cluster.fork( { TASK } ),
        cluster.fork( { TASK } ),
        cluster.fork( { TASK } ),
        cluster.fork( { TASK } ),
    ]

    workers.map( (w) => {
        w.on( 'message' , (message) => {
            if( message.method && message.method.length > 0 ){
                task[ message.method ]( message )
           }
        } )
        w.on( 'online' , () => { console.error( 'WORKER IS ONLINE' , w.id ) }  )
        w.on( 'listening' , () => { console.error( 'WORKER LSTENING' , w.id ) } )
        w.on( 'disconnect' , () => { console.error( 'WORKER DISCONNECTED' , w.id ) } )
        w.on( 'exit' , () => { console.error( 'WORKER EXIT' , w.id ) } )

    } )


    let lineCount = 0

    filter( SOURCE , task , ( line ) => {

        lineCount += 1
        workers[ lineCount % workers.length ].send( line )

    } )


} else {

    process.on( 'message' , ( line ) => {

        if( line != '[' && line != ']' ){

            let json = {}

            try{
                json = JSON.parse( line[line.length-1] == ',' ? line.slice(0,line.length-1) : line )
            }catch( e ){
                console.error( 'JSON PARSE ERROR' , e.message )
            }

            try{
                tasks[ process.env.TASK ].step( json , message => process.send(message) )
            }catch( e ){
                console.error( 'ERROR IN TASK' , e.message )
            }
        }

    })


}

