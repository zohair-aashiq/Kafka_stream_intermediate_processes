class EventsKafka {
    long id;
    long packageId;
    String timeStamp;
    String message;

    public EventsKafka(long id, long packageId, String timeStamp, String message) {
        this.id = id;
        this.packageId = packageId;
        this.timeStamp = timeStamp;
        this.message = message;
    }

    public long getID() {
        return this.id;
    }

    public long getPackageId() {
        return this.packageId;

    }

    public String getTimeStamp() {
        return this.timeStamp;

    }

    public String getMessage() {
        return this.message;

    }
}