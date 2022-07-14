package goodmetrics.downstream

import io.grpc.CallOptions
import io.grpc.Channel
import io.grpc.ClientCall
import io.grpc.ClientInterceptor
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall
import io.grpc.ForwardingClientCallListener.SimpleForwardingClientCallListener
import io.grpc.MethodDescriptor
import io.grpc.Status
import io.grpc.Metadata as GrpcMetadata

class GrpcTrailerLoggerInterceptor(
    private val onTrailers: (Status, GrpcMetadata) -> Unit
) : ClientInterceptor {
    override fun <ReqT, RespT> interceptCall(
        method: MethodDescriptor<ReqT, RespT>,
        callOptions: CallOptions,
        next: Channel
    ): ClientCall<ReqT, RespT> {
        return TrailersLoggingClientCall(next.newCall(method, callOptions), onTrailers)
    }

    private class TrailersLoggingClientCall<ReqT, RespT> (
        call: ClientCall<ReqT, RespT>?,
        private val onTrailers: (Status, GrpcMetadata) -> Unit,
    ) : SimpleForwardingClientCall<ReqT, RespT>(call) {
        override fun start(responseListener: Listener<RespT>, headers: GrpcMetadata) {
            super.start(MetadataCapturingClientCallListener(responseListener), headers)
        }

        private inner class MetadataCapturingClientCallListener(
            responseListener: Listener<RespT>?
        ) :
            SimpleForwardingClientCallListener<RespT>(responseListener) {
            override fun onHeaders(headers: GrpcMetadata) {
                super.onHeaders(headers)
            }

            override fun onClose(status: Status, trailers: GrpcMetadata?) {
                if (trailers != null && trailers.keys().isNotEmpty()) {
                    onTrailers(status, trailers)
                }
                super.onClose(status, trailers)
            }
        }
    }
}
