const CPU_COUNT = require('os').cpus().length

module.exports = {
    workers: [],
    roundRobin: 0,

    lastReadSpeeds: [],
    //maxSpeed: 0,
    lastOp: null,
    strikeDown: 0,

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
    },

    adjust( cluster , task , currentSpeed ){

        if( currentSpeed > 0 && this.lastReadSpeeds[0] != currentSpeed ){
            //if( readSpeed > maxSpeed ){ maxSpeed = readSpeed }
    
            this.lastReadSpeeds.unshift( currentSpeed )
            while( this.lastReadSpeeds.length > 2 ){
                this.lastReadSpeeds.pop()
            }
            let op = null
            if( this.lastReadSpeeds.length >= 2 ){
    
                if( /* / */ this.lastReadSpeeds[0] >= this.lastReadSpeeds[1] && this.lastOp == 'hire' ) {
                    op = 'hire'
    
                } else if( /* \ */ this.lastReadSpeeds[0] < this.lastReadSpeeds[1] && this.lastOp == 'hire' ){
                    this.strikeDown += 1
                    if( this.strikeDown >= 3 ){
                        op = 'fire'
                        this.strikeDown = 0
                    }
    
                } else if( /* /= */ this.lastReadSpeeds[0] >= this.lastReadSpeeds[1] && this.lastOp == 'fire' ) {
                    op = null
                    this.strikeDown = 0
    
                } else if( /* \ */ this.lastReadSpeeds[0] < this.lastReadSpeeds[1] && this.lastOp == 'fire' ){
                    op = 'hire'
                    this.strikeDown = 0
                }
    
            } else {
                op = 'hire'
            }
    
            if( op != null ){
                this[ op ]( cluster , task )
                this.lastOp = op
            }
        }
    }
}