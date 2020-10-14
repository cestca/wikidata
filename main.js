const SOURCE='D:\\wikidata\\latest-all.json\\latest-all.json'

//const cluster = require( 'cluster' )
const tasks = require( './tasks.js' )
const read = require( './reader.js' )


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

//if( cluster.isMaster ){

    let TASK = process.argv[2]

    if( TASK == null || TASK.trim().length == 0 || ! Object.keys(tasks).includes(TASK) ){
        console.error( 'bad input' , TASK )
        process.exit(1)
    }

    let task = tasks[TASK]

    // let workers = [
    //     cluster.fork( { TASK } ),
    //     cluster.fork( { TASK } ),
    //     cluster.fork( { TASK } ),
    //     cluster.fork( { TASK } ),
    //     cluster.fork( { TASK } ),
    // ]

    // workers.map( (w) => {
    //     w.on( 'message' , (message) => {
    //         if( message.method && message.method.length > 0 ){
    //             task[ message.method ]( message )
    //        }
    //     } )
    //     w.on( 'online' , () => { console.error( 'WORKER IS ONLINE' , w.id ) }  )
    //     w.on( 'listening' , () => { console.error( 'WORKER LSTENING' , w.id ) } )
    //     w.on( 'disconnect' , () => { console.error( 'WORKER DISCONNECTED' , w.id ) } )
    //     w.on( 'exit' , () => { console.error( 'WORKER EXIT' , w.id ) } )

    // } )


    let lineCount = 0

    read( SOURCE , task , ( line ) => {

        lineCount += 1

        //workers[ lineCount % workers.length ].send( line )
        
        work( TASK , line , ( message ) => {
            if( message.method && message.method.length > 0 ){
                task[ message.method ]( message )
           }

        } )

    } )




//} else {

    // process.on( 'message' , ( message ) => {
    //     work( TASK , message , )
    // } )


//}

