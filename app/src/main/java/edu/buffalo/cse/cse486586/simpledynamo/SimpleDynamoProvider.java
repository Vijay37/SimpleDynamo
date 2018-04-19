package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import org.apache.http.conn.ConnectTimeoutException;
import org.w3c.dom.Node;

import static android.content.ContentValues.TAG;

public class SimpleDynamoProvider extends ContentProvider {
    static final int SERVER_PORT = 10000;
    private final int timeout=500;
    private String server_reply="Done";
    private boolean query_started=false;
    private String my_node_no;
    private NodeInfo my_node_info;

// Array list and maps
    private String[] node_numbers={"5554","5556","5558","5560","5562"};
    private ArrayList<String> ring_list = new ArrayList<String>();
    private ArrayList<NodeInfo> nodes = new ArrayList<NodeInfo>();
    private HashMap<String,String> id_no_map = new HashMap<String, String>();
    private ArrayList<String> my_keys = new ArrayList<String>();
    private ArrayList<String> global_keys=new ArrayList<String>();
    private HashMap<String,String> global_values =new HashMap<String, String>();
// Array list and maps till here

//    Identifiers
    static final int port_mul_factor=2; // multiplication factor to get port number from emulator number
    static final String delimiter="`vh`"; // Delimiter string to separate and identify messages
    static final String curr_node_queryall="@"; // Identifier used to query all the keys stored in the current node
    static final String all_node_queryall="*"; // Identifier used to query the entire dht
    static final String newline_delimiter="`nl`";  // Delimiter to separate newline
    static final String other_seperator ="`as`"; // Delimiter to separate all values returned by a node when asked for key *
    static final String request_join="Node-Join"; // Node join message identifier
    static final String key_search="Data-Search"; // Data search message identifier
    static final String key_insert="Data-Insert"; // Key insert message identifier
    static final String key_result="Search-Result"; // identifier to tell node about the result it has queried
    private final String replica_identifier="`Replica`";
    static final String add_key="Add-Key"; // Identifier to tell node to add this key into its hash table (Content provider) used when a new node joins
    static final String del_key="Del-Key"; // Identifier to tell node to delete key
// Identifiers till here



	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
        if(selection.equals(curr_node_queryall)){
            delete_my_keys();
            my_keys.clear();
        }
        else if(selection.equals(all_node_queryall)){
            delete_my_keys();
            my_keys.clear();
            String forward_delete_msg=del_key+delimiter+selection;
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,forward_delete_msg,my_node_info.getSuc_1());
        }
        else
            process_delete_key(selection,my_node_no);
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub
        String key = values.get("key").toString();
        String value = values.get("value").toString();
        Log.v("Key to insert",key);
        Log.v("Value of the given key",value);
        process_insert(key,value);
		return null;
	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
        set_my_node_no();
		create_ring_structure();
		return false;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub
        MatrixCursor mc = process_query(selection);
		return mc;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}
    public void delete_key_in_my_node(String key){
        try {
            getContext().deleteFile(key);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public void process_delete_key(String key,String from_port){
	    NodeInfo node = find_key_location(key);
	    if(node.getMy_no().equals(my_node_no)){
	        delete_key_in_my_node(key);
            String forward_delete_msg=del_key+delimiter+key+delimiter+from_port+delimiter+replica_identifier;
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,forward_delete_msg,my_node_info.getSuc_1());
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,forward_delete_msg,my_node_info.getSuc_2());
        }
        else{
            String forward_delete_msg=del_key+delimiter+key+delimiter+from_port;
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,forward_delete_msg,node.getMy_no());
        }
    }
    public void delete_my_keys(){
        Log.v("Delete","Deleting my local keys");
        for(String key : my_keys)
            delete_key_in_my_node(key);
    }
	private MatrixCursor process_query(String key){
        String[] column_names ={"key","value"};
        MatrixCursor mc = new MatrixCursor(column_names);
	    try{
            if(key.equals(curr_node_queryall)){
                Log.v("Query","Querying "+key);
                String value="";
                for(String selection : my_keys) {
                    value=query_my_node(selection);
                    mc.newRow().add("key", selection).add("value", value.trim());
                }
            }
            else if(key.equals(all_node_queryall)){
                // write function to get all data
                mc = query_all_nodes(key);
            }
            else{
                mc = query_other_nodes(key);
            }
        }catch(Exception e){
	        e.printStackTrace();
        }
        return mc;
    }
    private MatrixCursor query_other_nodes(String selection){
        String[] column_names ={"key","value"};
        MatrixCursor mc = new MatrixCursor(column_names);
        String value="";
        try {
            NodeInfo node = find_key_location(selection);
            if (node.getSuc_2().equals(my_node_no)) {
                value = query_my_node(selection);
                mc.newRow().add("key", selection).add("value", value.trim());
            } else {
                String forward_search_msg = key_search + delimiter + selection + delimiter + my_node_no;
                query_started = true;
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, forward_search_msg, node.getSuc_2());
                synchronized (global_keys) {
                    while (query_started) {
                        global_keys.wait();
                    }
                    for (String key : global_keys) {
                        value = global_values.get(key);
                        mc.newRow().add("key", key).add("value", value.trim());
                    }
                    global_keys.clear();// resetting global_keys
                }
            }
        }catch(Exception e){
            e.printStackTrace();
        }
        return mc;
    }
    private MatrixCursor query_all_nodes(String selection){ // Querying all nodes
        String[] column_names ={"key","value"};
        MatrixCursor mc = new MatrixCursor(column_names);
        try{
            String value="";
            for(String key: my_keys){
                value=query_my_node(key);
                mc.newRow().add("key", key).add("value", value.trim());
            }
            String forward_search_msg = key_search + delimiter + selection+delimiter+my_node_no;
            query_started=true;
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,forward_search_msg,my_node_info.getSuc_1());
            synchronized (global_keys){
                while(query_started){
                    global_keys.wait();
                }
                for(String key : global_keys){
                    value = global_values.get(key);
                    mc.newRow().add("key", key).add("value", value.trim());
                }
                global_keys.clear();// resetting global_keys
            }
        }catch(Exception e){
            e.printStackTrace();
        }
        return mc;
    }
    public String query_my_node(String key){
        String value="";
        try {
            FileInputStream fis = getContext().openFileInput(key);
            BufferedReader br = new BufferedReader(new InputStreamReader(fis));
            String key_value;
            while ((key_value = br.readLine()) != null) {
                value = key_value;
            }
            br.close();
            fis.close();


        } catch (Exception e) {
            e.printStackTrace();
        }
        return value;
    }
	private void process_insert(String key, String value){
        try{
            NodeInfo node = find_key_location(key);
            String to_node_no = node.getMy_no();
            String suc_1="";
            String suc_2="";
            if(to_node_no.equals(my_node_no)){  // If the key belongs to me then store it in my node and two of my successors
                insert_in_my_node(key,value);
                String forward_msg = key_insert+delimiter+key+delimiter+value+delimiter+replica_identifier;
                suc_1=node.getSuc_1();
                suc_2=node.getSuc_2();
                send_insert_msg(forward_msg,suc_1);
                send_insert_msg(forward_msg,suc_2);
            }
            else{ // If it doesn't belong to me, send it to the guy to whom it belongs
                String forward_msg = key_insert+delimiter+key+delimiter+value;
                send_insert_msg(forward_msg,to_node_no);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    public boolean send_insert_msg(String msg, String to_node_no){
        String msgToSend = msg;
        String toPort = to_node_no;
        Socket socket;
        toPort=String.valueOf(Integer.parseInt(toPort)*port_mul_factor); // calulating actual exact port number
        PrintWriter out=null;
        BufferedReader bR=null;
        Log.v("Send insert message","Message to send : "+msgToSend);
        try {
            socket = new Socket();
            socket.connect(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(toPort)));
            socket.setSoTimeout(timeout);
            out = new PrintWriter(socket.getOutputStream(), true);
            out.println(msgToSend);
            bR = new BufferedReader(new InputStreamReader(socket.getInputStream())); // Read message from the socket
            if(bR==null){
                return false;
            }
        }catch (IOException e){
            e.printStackTrace();
            return false;
        }catch (Exception e){
            e.printStackTrace();
            return false;
        }
        return true;
    }
    public void insert_in_my_node(String key, String value){
        try{
            Log.v("Insert at my node","Key :"+key+" Value:"+value);
            FileOutputStream fos = getContext().openFileOutput(key, Context.MODE_PRIVATE);
            fos.write(value.getBytes());
            fos.close();
            my_keys.add(key);
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    private void create_ring_structure(){
        String hash_id;
        for(String node_no : node_numbers){
            try{
                hash_id=genHash(node_no);
                id_no_map.put(hash_id,node_no);
                ring_list.add(hash_id);
            }catch(Exception e){
                e.printStackTrace();
            }
        }
        Collections.sort(ring_list);
        create_nodes_info();
        create_server_task();
    }
    private void create_server_task(){
        ServerSocket serverSocket=null;
        try {
            serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            e.printStackTrace();
            Log.e(TAG, "Can't create a ServerSocket");
        }
    }
    private NodeInfo find_key_location(String key){
        NodeInfo node = null;
        String key_hash="";
        String node_id_hash="";
        String node_no="";
        try{
            key_hash=genHash(key);
            node_id_hash=ring_list.get(0);
            Log.v("Finding key","Started");
            if(key_hash.compareTo(node_id_hash)<=0 || key_hash.compareTo(ring_list.get(ring_list.size()-1))>0){ // If key is greater than everyone or smaller than everyone
                node_no=id_no_map.get(node_id_hash);
                node=find_node_by_node_no(node_no);
                Log.v("Finding key","First node");
                return node;
            }
            else{
                String prev_node_hash=node_id_hash;
                for(String temp_hash : ring_list){
                    if(key_hash.compareTo(prev_node_hash)>0 && key_hash.compareTo(temp_hash)<=0){
                        node_no=id_no_map.get(temp_hash);
                        node=find_node_by_node_no(node_no);
                        return node;
                    }
                }
            }

        }catch(Exception e){
            e.printStackTrace();
        }
        return node;
    }
    private NodeInfo find_node_by_node_no(String node_no){
	    NodeInfo temp_node=null;
	    for(NodeInfo node : nodes ){
	        if(node.getMy_no().equals(node_no)) {
                Log.v("Key location ",node.getMy_no());
                return node;
            }
        }

        return temp_node;
    }
    private class ServerTask extends AsyncTask<ServerSocket, String, Void> { // Server task to accept client connections and process their messages
        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            String message;
            String priority="";
            Log.v("Server task","created");
            BufferedReader bR=null;
            while(true){
                Socket client_sock=null;
                try {
                    // Source : https://docs.oracle.com/javase/tutorial/networking/sockets/clientServer.html
                    client_sock = serverSocket.accept();
                    PrintWriter out = null;
                    try {
                        bR = new BufferedReader(new InputStreamReader(client_sock.getInputStream()));
                        message = bR.readLine();
                        message = message.trim();
                        out = new PrintWriter(client_sock.getOutputStream(), true);
                        out.println(server_reply);
                        Log.v("Server task","message :"+message);
                        process_srvr_msg(message);

                    }catch(Exception e1){
                        e1.printStackTrace();
                        if(client_sock!=null)
                            client_sock.close();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
                finally {
                    if(bR!=null)
                        try {
                            bR.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }

                }

            }
        }
        private void process_srvr_msg(String msg){
            String[] format;
            format=msg.split(delimiter);
            if(format[0].equals(key_insert)){
                if(format.length==3)
                    process_srvr_insert(format[1],format[2],false);
                else
                    process_srvr_insert(format[1],format[2],true);
            }
            else if(format[0].equals(key_search)){
                process_query_serverside(format[1],format[2]);
            }
            else if(format[0].equals(key_result)){
                if(format.length!=3)
                    process_query_result(format[1],"");
                else
                    process_query_result(format[1],format[2]);
            }
//            else if(format[0].equals(add_key)){
//                process_distribute_keys(format[1]);
//            }
            else if(format[0].equals(del_key)){
                if(format.length==3)
                    process_delete_serverside(format[1],format[2],false);
                else
                    process_delete_serverside(format[1],format[2],true);
            }
        }
        public void process_delete_serverside(String selection,String from_port,boolean isRep){
            if(selection.equals(all_node_queryall)){
                delete_my_keys();
                if(!from_port.equals(my_node_info.getSuc_1())){
                    String forward_delete_msg=del_key+delimiter+selection+delimiter+from_port;
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,forward_delete_msg,my_node_info.getSuc_1());
                }
            }
            if(!isRep)
                process_delete_key(selection,from_port);
            else
                delete_key_in_my_node(selection);
        }
        public void process_query_result(String key, String value){
            synchronized (global_keys) {
                if(!key.equals(all_node_queryall)) {
                    global_keys.add(key);
                    global_values.put(key, value);
                    query_started = false;
                    global_keys.notify();
                }
                else{
                    String[] keys_set=value.split(newline_delimiter);
                    String[] key_line;
                    String recv_port="";
                    for(String splits : keys_set){
                        key_line=splits.split(other_seperator);
                        if(key_line.length>1){
                            global_keys.add(key_line[0]);
                            global_values.put(key_line[0],key_line[1]);
                        }
                        else{
                            recv_port=splits;
                            Log.v("Process Asterisk","Recevied values from :"+recv_port);
                        }
                    }
                    if(recv_port.equals(my_node_info.getPre_1())){
                        query_started=false;
                        global_keys.notify();
                    }

                }
            }
        }
        public void process_query_serverside(String key, String from_port){ // function to process received query request
            try {
                if(key.equals(all_node_queryall)){
                    // handle this scenario when asked for all keys
                    String return_msg=key_result+delimiter+key+delimiter;
                    String value="";
                    for(String selection : my_keys){
                        value=query_my_node(selection);
                        return_msg+=selection+ other_seperator +value+newline_delimiter;
                    }
                    return_msg+=my_node_no;
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, return_msg, from_port); // returning my keys to node who wants it
                    String forward_search_msg = key_search + delimiter + key+delimiter+from_port;
                    if(!my_node_info.getSuc_1().equals(from_port))
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, forward_search_msg, my_node_info.getSuc_1()); // forwarding the search to my successor

                }
                else {
                    send_my_answer(key,from_port);

                }
            }catch(Exception e){
                e.printStackTrace();
            }
        }
        public void send_my_answer(String key,String to_port){
            String value = query_my_node(key);
            String qu_res_msg = key_result+delimiter+key+delimiter+value;
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,qu_res_msg,to_port);
        }
        private void process_srvr_insert(String key, String value,boolean isRep){
            insert_in_my_node(key,value);
            if(!isRep){
                String my_suc1=my_node_info.getSuc_1();
                String my_suc2=my_node_info.getSuc_2();
                String forward_msg = key_insert+delimiter+key+delimiter+value+delimiter+replica_identifier;
                send_insert_msg(forward_msg,my_suc1);
                send_insert_msg(forward_msg,my_suc2);
            }
        }

        @Override
        protected void onProgressUpdate(String... values) {
            String message = values[0];
        }

    }
    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            String msgToSend = msgs[0];
            String toPort = msgs[1];
            Socket socket;
            toPort=String.valueOf(Integer.parseInt(toPort)*port_mul_factor); // calulating actual exact port number
            PrintWriter out=null;
            BufferedReader bR=null;
            Log.v("Client task","Message to send : "+msgToSend);
            try {
                socket = new Socket();
                socket.connect(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(toPort)));
                out = new PrintWriter(socket.getOutputStream(), true);
                out.println(msgToSend);
                bR = new BufferedReader(new InputStreamReader(socket.getInputStream())); // Read message from the socket
                if(bR==null){
                    // handle when node join fails
                }
            }catch (IOException e){
                e.printStackTrace();
            }catch (Exception e){
                e.printStackTrace();
            }
            return null;
        }
    }
    private void create_nodes_info(){
	    int one_shift=1;
	    int two_shift=2;
	    String temp_hash="";
	    int count=0;
	    int ring_size = ring_list.size();
	    int index_shift=1;
	    int max_ring_index=ring_size-index_shift;
        NodeInfo new_node;
	    String my_no="",my_pre1="",my_pre2="",my_suc1="",my_suc2="";
	    for(String hash_id : ring_list){
            my_no = id_no_map.get(hash_id);
            if(count-one_shift<0){
                temp_hash=ring_list.get(ring_size+count-one_shift);
            }
            else
                temp_hash=ring_list.get(count-one_shift);
            my_pre1 = id_no_map.get(temp_hash);
            if(count-two_shift<0){
                temp_hash=ring_list.get(ring_size+count-two_shift);
            }
            else
                temp_hash=ring_list.get(count-two_shift);
            my_pre2 = id_no_map.get(temp_hash);
            // Calc Successors
            if(count+one_shift>max_ring_index){
                temp_hash=ring_list.get(count+one_shift-max_ring_index-index_shift);
            }
            else
                temp_hash=ring_list.get(count+one_shift);
            my_suc1 = id_no_map.get(temp_hash);
            if(count+two_shift>max_ring_index){
                temp_hash=ring_list.get(count+two_shift-max_ring_index-index_shift);
            }
            else
                temp_hash=ring_list.get(count+two_shift);
            my_suc2 = id_no_map.get(temp_hash);
            new_node = new NodeInfo(my_no,my_pre1,my_pre2,my_suc1,my_suc2);
            nodes.add(new_node);
            if(new_node.getMy_no().equals(my_node_no)) // Storing my node information
                my_node_info=new_node;
            count++;
        }
        Log.v("My Node info",my_node_info.toString());
    }
    private void set_my_node_no(){
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        my_node_no = String.valueOf((Integer.parseInt(portStr)));
    }
    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
}
