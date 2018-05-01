package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
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

import static android.content.ContentValues.TAG;

public class SimpleDynamoProvider extends ContentProvider {
    static final int SERVER_PORT = 10000;
    private final int timeout=500;
    private String server_reply="Done";
    private boolean query_started=false;
    private String my_node_no;
    private NodeInfo my_node_info;
    private Object rw_mutex = new Object();
    private boolean rw_status = false;
    private boolean read_failed =false;
    // Array list and maps
    private String[] node_numbers={"5554","5556","5558","5560","5562"};
    private ArrayList<String> ring_list = new ArrayList<String>();
    private ArrayList<NodeInfo> nodes = new ArrayList<NodeInfo>();
    private HashMap<String,String> id_no_map = new HashMap<String, String>();
    private ArrayList<String> my_keys = new ArrayList<String>();
    private ArrayList<String> global_keys=new ArrayList<String>();
    private HashMap<String,String> global_values =new HashMap<String, String>();
    private HashMap<String,Boolean> nodes_status = new HashMap<String, Boolean>();
    private ArrayList<String> node_start_recv_lst = new ArrayList<String>();
// Array list and maps till here

    //    Identifiers
    static final int port_mul_factor=2; // multiplication factor to get port number from emulator number
    static final String delimiter="`vh`"; // Delimiter string to separate and identify messages
    static final String curr_node_queryall="@"; // Identifier used to query all the keys stored in the current node
    static final String all_node_queryall="*"; // Identifier used to query the entire dht
    static final String newline_delimiter="`nl`";  // Delimiter to separate newline
    static final String other_seperator ="`as`"; // Delimiter to separate all values returned by a node when asked for key *
    static final String key_search="Data-Search"; // Data search message identifier
    static final String key_insert="Data-Insert"; // Key insert message identifier
    static final String key_result="Search-Result"; // identifier to tell node about the result it has queried
    private final String replica_identifier="`Replica`";
    static final String add_key="Add-Key"; // Identifier to tell node to add this key into its hash table (Content provider) used when a new node joins
    static final String del_key="Del-Key"; // Identifier to tell node to delete key
    static final String ping_msg="ALIVE"; // Identifier to tell node that alive status has been sent
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
        clear_my_files();
        update_alive_status();
        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection,
                        String[] selectionArgs, String sortOrder) {
        // TODO Auto-generated method stub
        Log.v("Query Called","Key : "+selection);
        MatrixCursor mc = process_query(selection);
        return mc;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection,
                      String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }
    public void update_alive_status(){
        String alive_sts_msg=ping_msg+delimiter+my_node_no;
        new Update_live_status_task().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,alive_sts_msg);
    }
    public void clear_my_files(){
        Log.v("Clearing files","Started");
        File files_dir=getContext().getFilesDir();
        File[] files=files_dir.listFiles();
        for(File file : files){
            Log.v("Clearing files","File :"+file.getName());
            getContext().deleteFile(file.getName());
        }
    }
    public void delete_key_in_my_node(String key){
        try {
            if(my_keys.contains(key)) {
                getContext().deleteFile(key);

                my_keys.remove(key);
            }
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

                if(my_keys.isEmpty()) {
                    Log.v("@","no keys to return");
                    return mc;
                }
                Log.v("@","Taking lock");
                loop_until_rw_status_false();
                Log.v("@","Obtained lock");
                rw_status=true;
                synchronized (rw_mutex) {
                    for (String selection : my_keys) {
                        value = query_my_node(selection);
                        Log.v("@","Adding key"+selection+" Value:"+value);
                        mc.newRow().add("key", selection).add("value", value.trim());
                    }
                    rw_status=false;
                    rw_mutex.notify();
                }
                Log.v("@","Returning lock");
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
                if(my_keys.contains(selection)) {
                    Log.v("Query", "my node");
                    Log.v("Query", "Waiting for lock");
                    loop_until_rw_status_false(); // looping until the state is false
                    Log.v("Query", "Obtained lock");
                    rw_status = true; // changing status so no one else can access
                    synchronized (rw_mutex) {
                        value = query_my_node(selection);
                        rw_status = false; // changing status so others can access
                        rw_mutex.notify();
                        Log.v("Query", "Releasing lock");
                    }
                }
                else{
                    String forward_search_msg = key_search + delimiter + selection + delimiter + my_node_no;
                    query_started = true;
                    new QueryTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, forward_search_msg, node.getSuc_1());
                    synchronized (global_keys) {
                        while (query_started) {
                            global_keys.wait();
                        }
                        if(read_failed){
                            read_failed=false;
                            query_started=true;
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, forward_search_msg, node.getMy_no());
                            while (query_started) {
                                global_keys.wait();
                            }
                        }
                        for (String key : global_keys) {
                            value = global_values.get(key);
                            mc.newRow().add("key", key).add("value", value.trim());
                        }
                        global_keys.clear();// resetting global_keys
                    }
                }
                mc.newRow().add("key", selection).add("value", value.trim());
            } else {
                Log.v("Query","forward node");
                String forward_search_msg = key_search + delimiter + selection + delimiter + my_node_no;
                query_started = true;
                new QueryTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, forward_search_msg, node.getSuc_2());
                synchronized (global_keys) {
                    while (query_started) {
                        global_keys.wait();
                    }
                    if(read_failed){
                        read_failed=false;
                        query_started=true;
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, forward_search_msg, node.getSuc_1());
                        while (query_started) {
                            global_keys.wait();
                        }
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
            synchronized (global_keys) {
                for(String toPort : node_numbers){
                    if(!toPort.equals(my_node_no)){
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,forward_search_msg,toPort);
                    }
                }
                while (query_started) {
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
    private void loop_until_rw_status_false(){
        if(!rw_status)
            return;
        synchronized (rw_mutex) {
            while (rw_status) {
                try {
                    rw_mutex.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    private void process_insert(String key, String value){
        try{
            NodeInfo node = find_key_location(key);
            String to_node_no = node.getMy_no();
            String suc_1="";
            String suc_2="";
            suc_1=node.getSuc_1();
            suc_2=node.getSuc_2();
            loop_until_rw_status_false();
            if (to_node_no.equals(my_node_no)) {  // If the key belongs to me then store it in my node and two of my successors
                loop_until_rw_status_false();
                rw_status=true;
                synchronized (rw_mutex) {
                    insert_in_my_node(key, value);
                    rw_status=false;
                    rw_mutex.notify();
                }
                String forward_msg = key_insert + delimiter + key + delimiter + value + delimiter + replica_identifier;
                send_msg_to_client(forward_msg, suc_1);
                send_msg_to_client(forward_msg, suc_2);
            } else { // If it doesn't belong to me, send it to the guy to whom it belongs
                Log.v("Insert : ", "Forwarding key :" + key + " to the coordinator");
                String forward_msg = key_insert + delimiter + key + delimiter + value;
                send_msg_to_client(forward_msg,to_node_no);
                send_msg_to_client(forward_msg, suc_1);
                send_msg_to_client(forward_msg, suc_2);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        Log.v("Insert","Done");

    }
    public boolean send_msg_to_client(String msg, String to_node_no){
        String msgToSend = msg;
        String toPort = to_node_no;
        Socket socket=null;
        toPort=String.valueOf(Integer.parseInt(toPort)*port_mul_factor); // calculating actual exact port number
        PrintWriter out=null;
        BufferedReader bR=null;
        String reply="";
        Log.v("Send insert message","Message to send : "+msgToSend+ "To port :"+toPort);
        try {
            socket = new Socket();
            socket.connect(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(toPort)));
            out = new PrintWriter(socket.getOutputStream(), true);
            out.println(msgToSend);
            bR = new BufferedReader(new InputStreamReader(socket.getInputStream())); // Read message from the socket
            reply = bR.readLine();
            if(!reply.equals(server_reply)){
                Log.e("Node failure (send)",to_node_no+" "+reply);
                return false;
            }
            else{
                Log.v("Client ","Message sent successfully");
            }
            bR.readLine();
        }catch (IOException e){
            e.printStackTrace();
            return false;
        }catch (Exception e){
            e.printStackTrace();
            return false;
        }finally {
            try {
                if (out != null)
                    out.close();
                if (bR != null)
                    bR.close();
                if (socket != null)
                    socket.close();
            }catch(Exception e1){
                e1.printStackTrace();
            }

        }
        return true;
    }
    public void insert_in_my_node(String key, String value){
        try{
            Log.v("Insert at my node","Key :"+key+" Value:"+value);
            FileOutputStream fos = getContext().openFileOutput(key, Context.MODE_PRIVATE);
            fos.write(value.getBytes());
            fos.close();
            if(!my_keys.contains(key))
                my_keys.add(key);
            else
                Log.v("Insert at my node : ","Duplicate key entry");
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
            if(key_hash.compareTo(node_id_hash)<=0 || key_hash.compareTo(ring_list.get(ring_list.size()-1))>0){ // If key is greater than everyone or smaller than everyone
                node_no=id_no_map.get(node_id_hash);
                node=find_node_by_node_no(node_no);
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
                    prev_node_hash=temp_hash;
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
                return node;
            }
        }

        return temp_node;
    }
    private class Update_live_status_task extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            String msgToSend = msgs[0];
            Socket socket=null;

            PrintWriter out=null;
            BufferedReader bR=null;
            String message="";
            Log.v("Update Alive status","Message to send : "+msgToSend);
            try {
                for(String toPort : node_numbers) {
                    if(toPort.equals(my_node_no) || toPort.equals(my_node_info.getSuc_2())) // Don't send it to me and my tail
                        continue;
                    toPort=String.valueOf(Integer.parseInt(toPort)*port_mul_factor); // calulating actual exact port number
                    socket = new Socket();
                    socket.connect(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(toPort)));
                    out = new PrintWriter(socket.getOutputStream(), true);
                    out.println(msgToSend);
                    bR = new BufferedReader(new InputStreamReader(socket.getInputStream())); // Read message from the socket
                    message = bR.readLine();
                    if (message == null) {
                        // handle when node join fails
                    }
                    Log.v("Update Alive Status","Status sent to :"+toPort);
                }
            }catch (IOException e){
                e.printStackTrace();
            }catch (Exception e){
                e.printStackTrace();
            }
            finally {
                if(socket !=null) {
                    try {
                        socket.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }

            }
            return null;
        }
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

                        Log.v("Server task","message :"+message);
                        out.println(server_reply);
                        process_srvr_msg(message);
                        out.println(server_reply);

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
                process_query_server_side(format[1],format[2]);
            }
            else if(format[0].equals(key_result)){
                if(format.length!=3)
                    process_query_result(format[1],"");
                else
                    process_query_result(format[1],format[2]);
            }
            else if(format[0].equals(ping_msg)){
                process_alive_info(format[1]);
            }
            else if(format[0].equals(add_key)){
                process_distribute_keys(format[1],format[2]);
            }
            else if(format[0].equals(del_key)){
                if(format.length==3)
                    process_delete_server_side(format[1],format[2],false);
                else
                    process_delete_server_side(format[1],format[2],true);
            }
        }
        public void process_distribute_keys(String keys,String from_port){
            try {
                String splits[] = keys.split(newline_delimiter);
                String key_splits[];
                String key = "";
                String value = "";
                NodeInfo node = null;
                Log.v("D_Keys Process", from_port);
                Log.v("D_keys_Process","Taking lock");
                loop_until_rw_status_false();
                Log.v("D_keys_Process","Obtained lock");
                rw_status=true;
                String temp_value="";
                synchronized (rw_mutex) {
                    for (String splitter : splits) {
                        if (splitter.trim() != "") {
                            key_splits = splitter.split(other_seperator);
                            key = key_splits[0];
                            value = key_splits[1];
                            Log.v("D_Process","Processing "+key);
                            temp_value=query_my_node(key);
                            if(temp_value.isEmpty() || temp_value==null)
                                insert_in_my_node(key, value);

                        }
                    }
                    rw_status=false;
                    rw_mutex.notify();
                    Log.v("D_Process","Releasing lock");
                }
            }catch(Exception e){
                e.printStackTrace();
            }
        }
        public void process_alive_info(String from_port){
            distribute_keys(from_port);
        }
        public void distribute_keys(String from_port){
            try {
                String dist_key_msg=add_key+delimiter;
                String value="";
                NodeInfo node=null;
                boolean is_suc=false;
                if(my_node_info.getSuc_2().equals(from_port) || my_node_info.getSuc_1().equals(from_port)){ // If the new node is one of my successors
                    is_suc=true;
                }
                loop_until_rw_status_false();
                rw_status=true;
                synchronized (rw_mutex) {
                    for (String key : my_keys) {
                        node = find_key_location(key);
                        if (node.getMy_no().equals(from_port) && !is_suc) {  // send if key belongs to the new node and it is not my successor
                            value = query_my_node(key);
                            dist_key_msg += key + other_seperator + value + newline_delimiter;
                        } else if (node.getMy_no().equals(my_node_no) && is_suc) { // send if key belongs to me and the new node is one of my successors
                            value = query_my_node(key);
                            dist_key_msg += key + other_seperator + value + newline_delimiter;
                        }
                    }
                    rw_status=false;
                    rw_mutex.notify();
                }
                dist_key_msg+=delimiter+my_node_no;
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,dist_key_msg,from_port);
            }catch (Exception e){
                e.printStackTrace();
            }

        }
        public void process_delete_server_side(String selection, String from_port, boolean isRep){
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
                    node_start_recv_lst.add(recv_port);
                    if(ready_to_return_asterisk()){
                        query_started=false;
                        global_keys.notify();
                    }

                }
            }
        }
        public boolean ready_to_return_asterisk(){
            int min_list_size = 3;
            int accepted_list_size = 4;
            Log.v("Ready to return : ","Size : "+node_start_recv_lst.size());
            if(node_start_recv_lst.size() < min_list_size)
                return false;
            else if(node_start_recv_lst.size()==accepted_list_size)
                return true;
            else{
                for(String node_no : node_numbers){
                    if(!node_no.equals(my_node_no)) {
                        if (!nodes_status.get(node_no)) {
                            return true;
                        }
                    }
                }
            }

            return false;
        }
        public void process_query_server_side(String key, String from_port){ // function to process received query request
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

                }
                else {
                    send_my_answer(key,from_port);

                }
            }catch(Exception e){
                e.printStackTrace();
            }
        }
        public void send_my_answer(String key,String to_port){
            if(!my_keys.contains(key)){
                Log.v("Query-Server","Could not find the key");
                NodeInfo node = find_key_location(key);
                String forward_search_msg = key_search + delimiter + key + delimiter + to_port;
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, forward_search_msg, node.getSuc_1());
            }
            else {
                Log.v("Query-server", "Waiting for the lock");
                loop_until_rw_status_false();
                Log.v("Query-server", "Obtained the lock");
                rw_status = true;
                synchronized (rw_mutex) {
                    String value = query_my_node(key);
                    Log.v("Query-server", "Releasing the lock");
                    rw_status = false;
                    rw_mutex.notify();
                    String qu_res_msg = key_result + delimiter + key + delimiter + value;
                    Log.v("Server : ", "Replying answer for :" + key);
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, qu_res_msg, to_port);
                }
            }

        }
        private void process_srvr_insert(String key, String value,boolean isRep){
            Log.v("Insert","Processing server side");
            loop_until_rw_status_false();
            Log.v("Insert-Server","Obtained the lock");
            rw_status=true;
            synchronized (rw_mutex) {
                insert_in_my_node(key, value);
                rw_status = false;
                rw_mutex.notify();
                Log.v("Insert-Server","Releasing lock");
            }
            Log.v("Insert-Server","Insertion done");

        }

        @Override
        protected void onProgressUpdate(String... values) {
            String msg = values[0];
        }

    }
    private class QueryTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            String msgToSend = msgs[0];
            String toPort = msgs[1];
            Socket socket=null;
            toPort=String.valueOf(Integer.parseInt(toPort)*port_mul_factor); // calulating actual exact port number
            PrintWriter out=null;
            BufferedReader bR=null;
            String message="";
            Log.v("Query task","Message to send : "+msgToSend+ "To Port : "+toPort);
            boolean status = true;
            try {
                socket = new Socket();
                socket.connect(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(toPort)));
                out = new PrintWriter(socket.getOutputStream(), true);
                out.println(msgToSend);
                bR = new BufferedReader(new InputStreamReader(socket.getInputStream())); // Read message from the socket
                message = bR.readLine();
                if(!message.equals(server_reply)){
                    status=false;
                }
            }catch (IOException e){
                e.printStackTrace();
                status=false;
            }catch (Exception e){
                e.printStackTrace();
                status=false;
            }finally {
                toPort = String.valueOf(Integer.parseInt(toPort)/port_mul_factor);
                if (!status) {
                    nodes_status.put(toPort,false);
                    synchronized (global_keys) {
                        query_started = false;
                        read_failed = true;
                        global_keys.notify();
                    }

                } else {
                    nodes_status.put(toPort,true);
                }
                if(socket !=null) {
                    try {
                        socket.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
            return null;
        }
    }
    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            String msgToSend = msgs[0];
            String toPort = msgs[1];
            Socket socket=null;
            toPort=String.valueOf(Integer.parseInt(toPort)*port_mul_factor); // calulating actual exact port number
            PrintWriter out=null;
            BufferedReader bR=null;
            String message="";
            Log.v("Client task","Message to send : "+msgToSend+ "To Port : "+toPort);
            boolean status = true;
            try {
                socket = new Socket();
                socket.connect(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(toPort)));
                out = new PrintWriter(socket.getOutputStream(), true);
                out.println(msgToSend);
                bR = new BufferedReader(new InputStreamReader(socket.getInputStream())); // Read message from the socket
                message = bR.readLine();
                if(!message.equals(server_reply)){
                    status=false;
                }
            }catch (IOException e){
                e.printStackTrace();
                status=false;
            }catch (Exception e){
                e.printStackTrace();
                status=false;
            }finally {
                toPort = String.valueOf(Integer.parseInt(toPort)/port_mul_factor);
                if (!status) {
                    nodes_status.put(toPort,false);
                } else {
                    nodes_status.put(toPort,true);
                }
                if(socket !=null) {
                    try {
                        socket.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
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
