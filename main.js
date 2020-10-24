const SOURCE = 'D:\\wikidata\\latest-all.json\\latest-all.json'
const IS_CLUSTER = true
//let CLUSTER_WORKERS = 2

const cluster = IS_CLUSTER ? require( 'cluster' ) : null
const tasks = require( './tasks.js' )
const read = require( './reader.js' )

let TASK = process.argv[2]

if( TASK == null || TASK.trim().length == 0 || ! Object.keys(tasks).includes(TASK) ){
    console.error( 'bad input' , TASK )
    process.exit(1)
}

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


const Worker = {
    all: [],
    roundRobin: 0,

    pick(){
        let worker = this.all[ this.roundRobin % this.all.length ]
        this.roundRobin += 1
        return worker
    },

    // idle(){},

    hire(){
        let worker = cluster.fork()
        worker.index = this.all.length
        worker.on( 'message' , (message) => { task.handleMessage(message) } )
        worker.on( 'online' , () => { console.error( 'WORKER IS ONLINE' , worker.index ) }  )
        worker.on( 'listening' , () => { console.error( 'WORKER LISTENING' , worker.index ) } )
        //worker.on( 'disconnect' , () => { console.error( 'WORKER DISCONNECTED' , worker.index ) } )
        worker.on( 'exit' , () => { console.error( 'WORKER EXIT' , worker.index ) } )
        this.all.push( worker )
    },

    fire(){
        if( this.all.length > 1 ){
            let worker = this.all.pop()
            worker.send( { method: 'exit' } )    
        }
    }
}

// const adjustCluster = ( workerCount ) => {
//     while( workers.length < workerCount ){
//         createWorker()
//     }
//     while( workers.length > workerCount ){
//         removeWorker()
//     }
// }

if( ! IS_CLUSTER || cluster.isMaster ){

    if( IS_CLUSTER ){
        Worker.hire()
        //Worker.hire()

        //adjustCluster( CLUSTER_WORKERS )
    }

//    let lineCount = 0
//    let speeds = []
//    let lastReadSpeed = 0
    let lastReadSpeeds = []
    // let lastOps = []
    // let maxSpeed = 0

    let lastOp = null
    let strikeDown = 0
    let strikeUp = 0

    if( task.start ){ task.start() }

    read( SOURCE , ( line , readSpeed ) => {

//        lineCount += 1

        if( IS_CLUSTER ){

            /* ADJUST CLUSTER - TRY 2 */
            if( readSpeed > 0 && lastReadSpeeds[0] != readSpeed ){
                //if( readSpeed > maxSpeed ){ maxSpeed = readSpeed }
                lastReadSpeeds.unshift( readSpeed )
                while( lastReadSpeeds.length > 2 ){
                    lastReadSpeeds.pop()
                }
                let op = null
                if( lastReadSpeeds.length >= 2 ){

                    if( lastReadSpeeds[0] > lastReadSpeeds[1] && lastOp == 'hire' ) {
                        op = 'hire'

                    } else if( lastReadSpeeds[0] < lastReadSpeeds[1] && lastOp == 'hire' ){
                        strikeDown += 1
                        if( strikeDown >= 3 ){
                            op = 'fire'
                            strikeDown = 0
                        }

                    } else if( lastReadSpeeds[0] >= lastReadSpeeds[1] && lastOp == 'fire' ) {
                        op = null
                        strikeDown = 0

                    } else if( lastReadSpeeds[0] < lastReadSpeeds[1] && lastOp == 'fire' ){
                        op = 'hire'
                        strikeDown = 0
                    }

                } else {
                    op = 'hire'
                }

                if( op != null ){
                    Worker[ op ]()
                    lastOp = op
                }

                // lastOps.unshift( op )
                // while( lastOps.length > 5  ){
                //     lastOps.pop()
                // }
            }
            /* */

            /* ADJUST CLUSTER - TRY 1 * /
            if( readSpeed > 0 && readSpeed != lastReadSpeed ){
                speeds[ CLUSTER_WORKERS ] = readSpeed
                console.error( 'tick' , new Date() , readSpeed , lastReadSpeed , CLUSTER_WORKERS , speeds[ CLUSTER_WORKERS ] , speeds )
                lastReadSpeed = readSpeed
                if( speeds[ CLUSTER_WORKERS - 1 ] && speeds[ CLUSTER_WORKERS - 1 ] > ( speeds[ CLUSTER_WORKERS ] * 0.8 ) ){
                    adjustCluster( CLUSTER_WORKERS - 1 )
                } else if( speeds[ CLUSTER_WORKERS + 1 ] && speeds[ CLUSTER_WORKERS + 1 ] > ( speeds[ CLUSTER_WORKERS ] * 1.2 ) ){
                    adjustCluster( CLUSTER_WORKERS + 1 )
                } else if( speeds[ CLUSTER_WORKERS + 1 ] == null ){
                    adjustCluster( CLUSTER_WORKERS + 1 )
                }
            }
            /* */

            Worker.pick().send( line )

        } else {
            work( line , ( message ) => { task.handleMessage( message ) } )

        }

    } ).then( () => {

        if( task.end ){ task.end() }

        Worker.all.forEach( w => {
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

