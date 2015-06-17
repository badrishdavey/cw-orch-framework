package com.carewise.pentaho.api;



import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;

/**
 * Created by bdavay on 6/15/15.
 */
public class PentahoTransformationExecutor {


        public static void main( String[] args )
        {
            try {
                KettleEnvironment.init();
                TransMeta metaData = new TransMeta(PentahoTransformationExecutor.class.getResource("/transformation.ktr").toString());
                Trans trans = new Trans( metaData );
                trans.execute( null );
                trans.waitUntilFinished();
                if ( trans.getErrors() > 0 ) {
                    System.out.print( "Error Executing transformation" );
                }
            } catch( KettleException e ) {
                e.printStackTrace();
            }
        }


}
