
module.exports.handleMessage = function( message ){
    if( message.method && message.method.length > 0 ){
        this[ message.method ]( message )
   }
}