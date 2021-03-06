const MAX_SCAN_SIZE = 0 // 10 * 1024 * 1024 * 1024
const SCAN_SPEED_PERIOD = 2 // seconds

const fs = require( 'fs' )
const readline = require( 'readline' )
const stream = require( 'stream' )

module.exports = ( source , sendLine ) => new Promise( (resolve,reject) => {

    let inStream = fs.createReadStream( source );
    let outStream = new stream();
    let reader = readline.createInterface(inStream, outStream);
    
    let lineCount = 0
    let scanSize = 0
    let scanStart = new Date()
    let scanSpeed = 0
    let scanSpeedBuffer = 0
    let scanSpeedStart = Date.now()
    
    console.error( 'SCAN START' , scanStart.toString() )
    
    reader.on( 'SIGINT' , function(){ console.error( 'READER SIGINT' , lineCount ) } )
    reader.on( 'SIGCONT' , function(){ console.error( 'READER SIGCONT' , lineCount ) } )
    reader.on( 'SIGTSTP' , function(){ console.error( 'READER SIGTSTP' , lineCount ) } )
    reader.on('pause', function(){ console.error( 'READER PAUSED' ) } )
    reader.on('resume', function(){ console.error( 'READER RESUME' ) } )
    
    reader.on('close', function() {
        console.error( 'END OF FILE' )
        let scanEnd = new Date()
        let period = Math.round( (scanEnd - scanStart) / 1000 )
        let scanMeanSpeed = Math.round( scanSize / period )
        console.error( 'SCAN END' , scanEnd.toString() )
        console.error( 'duration (seconds)' , period )
        console.error( 'line count' , lineCount )
        console.error( 'scan size (bytes)' , scanSize )
        console.error( 'scan mean speed (B/s)' , scanMeanSpeed )
        resolve()
    })
    
    reader.on( 'line' , function( line ) {
    
        if( reader.closed ){ return }
    
        if( MAX_SCAN_SIZE > 0 && scanSize > MAX_SCAN_SIZE ){
            reader.close()
            console.error( 'max scan size, close' , scanSize )
        }

        sendLine( line , scanSpeed )

        lineCount += 1
        
        let size = Buffer.byteLength( line )
        scanSize += size
        scanSpeedBuffer += size
    
        let period = Math.round( ( Date.now() - scanSpeedStart ) / 1000 )
    
        if( period > SCAN_SPEED_PERIOD ){
            scanSpeed = Math.round( scanSpeedBuffer / period )
            scanSpeedBuffer = 0
            scanSpeedStart = Date.now()
        }
    
    })

} )