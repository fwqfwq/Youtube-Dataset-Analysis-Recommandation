package MRUserSearch;

public class User {
	
	final String user_id;

    public User(String user_id) {
        this.user_id = user_id;
    }

    @Override
    public String toString() {
        return this.user_id;
    }
}
