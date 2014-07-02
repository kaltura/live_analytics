Installation guide:
-------------------
1. Install tomcat (preferably 8)
2. Configure Tomcat (see section 'TomCat Configuration')
3. Export WAR file (Soon to be a maven script)
4. Deploy the war {$TOMCAT}/webapps
5. Verify web service was deplyed succesfully by entering - http://<tom-cat host>:<tomcat port>/KalturaLiveAnalytics/KalturaLiveAnalytics
6. Execute the test client.


Tomcat configuration:
----------------------
1. Go here http://jax-ws.java.net/.
2. Download JAX-WS RI distribution.
3. Unzip it and copy following JAX-WS dependencies to Tomcat library folder “{$TOMCAT}/lib“.
	gmbal-api-only.jar
	ha-api.jar
	jaxb-core.jar
	jaxb-impl.jar
	jaxws-api.jar
	jaxws-rt.jar
	management-api.jar
	policy.jar
	stax-ex.jar
	streambuffer.jar