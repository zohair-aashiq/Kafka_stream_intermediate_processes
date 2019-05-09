class PackageKafka {
    String packageID;
    String eventsIds;


    public PackageKafka(String packageID, String eventsIds) {
        this.packageID = packageID;
        this.eventsIds = eventsIds;
    }

    public String getID() {
        return this.packageID;
    }

    public String getEventsIds() {
        return this.eventsIds;

    }

}