const CPU_COUNT = require('os').cpus().length

module.exports = {
    workers: [],
    roundRobin: 0,

    pick(){
        let worker = this.workers[ this.roundRobin % this.workers.length ]
        this.roundRobin += 1
        return worker
    },

    // idle(){},

    hire( cluster , task ){
        if( ( this.workers.length + 1 ) <= CPU_COUNT ){

            let worker = cluster.fork()
            worker.index = this.workers.length
            worker.on( 'message' , (message) => { task.handleMessage(message) } )
            worker.on( 'online' , () => { console.error( 'WORKER IS ONLINE' , worker.index ) }  )
            worker.on( 'listening' , () => { console.error( 'WORKER LISTENING' , worker.index ) } )
            //worker.on( 'disconnect' , () => { console.error( 'WORKER DISCONNECTED' , worker.index ) } )
            worker.on( 'exit' , () => { console.error( 'WORKER EXIT' , worker.index ) } )
            this.workers.push( worker )

        } else {
            console.error( 'need more cpu' )
        }
    },

    fire(){
        if( this.workers.length > 1 ){
            let worker = this.workers.pop()
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