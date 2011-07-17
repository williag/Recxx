package org.recxx.facades;

import org.recxx.Recxx;

import java.util.Properties;

/**
 * Defines a contract that all rec worker objects must implement to be able to
 * be used by the Reconciliation framework.
 */
public interface RecxxWorker extends Runnable {

    /**
     * run the job
     */
    public void run();

    /**
     * set the properties object to be used tp configure this worker
     *
     * @param p - configured Properties object
     */
    public void setRunTimeProperties(Properties p);

    /**
     * Method setDataStore.
     *
     * @param db set the database to use.
     */
    public void setDataStore(Recxx db);

}
