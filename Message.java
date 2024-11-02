public class Message{
    private final long offset;
    private final String content;

    public Message(long offset, String content){
        this.offset = offset;
        this.content = content;
    }
    public long getOffset(){
        return offset;
    }
    public String getContent(){
        return content;
    }
}