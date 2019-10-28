package de.unikassel.vs.pdDebug;

import com.ochafik.lang.jnaerator.runtime.NativeSize;
import com.sun.jna.Memory;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.IntByReference;
import de.unikassel.vs.pdDebug.capnzero.Capnzero;
import de.unikassel.vs.pdDebug.libzmq.zmq_msg_t;

import static de.unikassel.vs.pdDebug.libzmq.LibZMQLibrary.*;

public class Subscriber {
    private int MESSAGE_OFFSET = 24;
    final boolean DEBUG = false;

    private Protocol protocol = Protocol.UDP;
    private String groupName = "TestGroupName";
    private Pointer socket;
    private Pointer context;

    public Subscriber() {
        this(INSTANCE.zmq_ctx_new());
    }

    Subscriber(Pointer context) {
        this.context = context;
    }

    public void destroy() {
        check(INSTANCE.zmq_close(socket), "zmq_close");
    }

    public void term() {
        check(INSTANCE.zmq_ctx_term(context), "zmq_ctx_term");
    }

    public void subscribe(Protocol protocol, String address) {
        this.protocol = protocol;

        IntByReference timeout = new IntByReference(500);
        NativeSize optValLen = new NativeSize(4);

        String emptyString = "";
        Pointer m = new Memory(emptyString.length() + 1); // WARNING: assumes ascii-only string
        m.setString(0, emptyString);
        NativeSize optValLenM = new NativeSize(0);

        switch (protocol) {
        case UDP:
            socket = INSTANCE.zmq_socket(context, ZMQ_DISH);
            check(INSTANCE.zmq_setsockopt(socket, ZMQ_RCVTIMEO, timeout.getPointer(), optValLen), "zmq_setsockopt");
            check(INSTANCE.zmq_join(socket, groupName), "zmq_join");
            check(INSTANCE.zmq_bind(socket, "udp://" + address), "zmq_bind");
            break;
        case TCP:
            socket = INSTANCE.zmq_socket(context, ZMQ_SUB);
            check(INSTANCE.zmq_setsockopt(socket, ZMQ_RCVTIMEO, timeout.getPointer(), optValLen), "zmq_setsockopt");
            check(INSTANCE.zmq_setsockopt(socket, ZMQ_SUBSCRIBE, m, optValLenM), "zmq_setsockopt");
            check(INSTANCE.zmq_connect(socket, "tcp://" + address), "zmq_connect");
            break;
        case IPC:
            socket = INSTANCE.zmq_socket(context, ZMQ_SUB);
            check(INSTANCE.zmq_setsockopt(socket, ZMQ_RCVTIMEO, timeout.getPointer(), optValLen), "zmq_setsockopt");
            check(INSTANCE.zmq_setsockopt(socket, ZMQ_SUBSCRIBE, m, optValLenM), "zmq_setsockopt");
            check(INSTANCE.zmq_connect(socket, "ipc://" + address), "zmq_connect");
            break;
        default:
            socket = null;
        }

    }

    public void start(final int frequenzy, final boolean serialized) {
        Thread t1 = new Thread(new Runnable() {
            public void run() {

                try {
                    for (int i = 0; i < frequenzy; i++) {
                        Thread.sleep(1000);

                        if (serialized) {
                            getSerializedMessage();
                        } else {
                            getMessage();
                        }
                    }

                    // wait to receive the last messages
                    Thread.sleep(frequenzy);
                    destroy();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        t1.start();
    }

    private void check(int returnCode, String nameOfMethod) {
        if (DEBUG && returnCode == 0) {
            System.out.println(nameOfMethod + " returned: " + returnCode);
        }
        if (returnCode != 0) {
            System.err.println(nameOfMethod + " returned: " + returnCode);
        }
    }

    public String getSerializedMessage() {
        String message = Capnzero.receiveSerializedMessage(socket, protocol.ordinal());
        return message;
    }

    public String getMessage() {
        String msg_str = "";
        zmq_msg_t msg = new zmq_msg_t();
        check(INSTANCE.zmq_msg_init(msg), "zmq_msg_init");

        int bytes = INSTANCE.zmq_msg_recv(msg, socket, 0);

        if (bytes > 0) {
            Pointer messageData = INSTANCE.zmq_msg_data(msg);
            int messageSize = INSTANCE.zmq_msg_size(msg).intValue();

            int realMessageSize = messageSize - MESSAGE_OFFSET;
            byte[] messageBytes = new byte[realMessageSize];
            messageData.read(MESSAGE_OFFSET, messageBytes, 0, realMessageSize);
            msg_str = new String(messageBytes).replaceAll("\u0000", "");
        }
        System.out.println();
        check(INSTANCE.zmq_msg_close(msg), "zmq_msg_close");

        return msg_str;
    }

    public Pointer getContext() {
        return context;
    }

    public void setContext(Pointer context) {
        this.context = context;
    }

    public Pointer getSocket() {
        return socket;
    }

    public Protocol getProtocol() {
        return protocol;
    }

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }
}
