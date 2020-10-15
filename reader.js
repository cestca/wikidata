const MAX_SCAN_SIZE = 10 * 1024 * 1024 //* 1024
const SCAN_SPEED_PERIOD = 60 // seconds

const fs = require( 'fs' )
const readline = require( 'readline' )
const stream = require( 'stream' )

module.exports = ( SOURCE , task , sendWork ) => {

    let inStream = fs.createReadStream( SOURCE );
    let outStream = new stream();
    let reader = readline.createInterface(inStream, outStream);
    
    let lineCount = 0
    let scanSize = 0
    let scanStart = new Date()
    let scanSpeed = 0
    let scanSpeedBuffer = 0
    let scanSpeedStart = Date.now()
    
    console.error( 'SCAN START' , scanStart.toString() )
    if( task.start ){ task.start() }
    
    reader.on( 'SIGINT' , function(){ console.error( 'READER SIGINT' , lineCount ) } )
    reader.on( 'SIGCONT' , function(){ console.error( 'READER SIGCONT' , lineCount ) } )
    reader.on( 'SIGTSTP' , function(){ console.error( 'READER SIGTSTP' , lineCount ) } )
    reader.on('pause', function(){ console.error( 'READER PAUSED' ) } )
    reader.on('resume', function(){ console.error( 'READER RESUME' ) } )
    
    reader.on('close', function() {
        //console.log( '}' )
        console.error( 'END OF FILE' )
        task.end()
        let scanEnd = new Date()
        let period = Math.round( (scanEnd - scanStart) / 1000 )
        let scanMeanSpeed = Math.round( scanSize / period )
        console.error( 'SCAN END' , scanEnd.toString() )
        console.error( 'duration (seconds)' , period )
        console.error( 'line count' , lineCount )
        console.error( 'scan size (bytes)' , scanSize )
        console.error( 'scan mean speed (B/s)' , scanMeanSpeed )
        process.exit(0)
    })
    
    reader.on( 'line' , function( line ) {
    
        if( reader.closed ){ return }
    
        if( MAX_SCAN_SIZE > 0 && scanSize > MAX_SCAN_SIZE ){
            reader.close()
            console.error( 'max scan size, close' , scanSize )
        }

        sendWork( line )

        lineCount += 1
        
        let size = Buffer.byteLength( line )
        scanSize += size
        scanSpeedBuffer += size
    
        let period = Math.round( ( scanSpeedStart - Date.now() ) / 1000 )
    
        if( period > SCAN_SPEED_PERIOD ){
            scanSpeed = Math.round( scanSpeedBuffer / period )
            scanSpeedBuffer = 0
            scanSpeedStart = Date.now()
        }
    
    })

}