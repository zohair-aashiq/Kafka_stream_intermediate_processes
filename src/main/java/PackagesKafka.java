class PackagesKafka {
    long id;
    long[] eventsIds;
    String message;


    public PackagesKafka(long id, long[] eventsIds, String message) {
        this.id = id;
        this.eventsIds = eventsIds;
        this.message = message;
    }

    public long getID() {
        return this.id;
    }

    public long[] getEventsIds() {
        return this.eventsIds;

    }

    public String getMessage() {
        return this.message;

    }


}