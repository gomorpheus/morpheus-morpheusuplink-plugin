package com.morpheusdata.micetro

import com.morpheusdata.core.util.HttpApiClient
import com.morpheusdata.core.util.NetworkUtility
import com.morpheusdata.core.DNSProvider
import com.morpheusdata.core.IPAMProvider
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.Plugin
import com.morpheusdata.core.util.ConnectionUtils
import com.morpheusdata.core.util.SyncTask
import com.morpheusdata.model.AccountIntegration
import com.morpheusdata.model.Icon
import com.morpheusdata.model.NetworkDomain
import com.morpheusdata.model.NetworkDomainRecord
import com.morpheusdata.model.NetworkPool
import com.morpheusdata.model.NetworkPoolIp
import com.morpheusdata.model.NetworkPoolRange
import com.morpheusdata.model.NetworkPoolServer
import com.morpheusdata.model.NetworkPoolType
import com.morpheusdata.model.OptionType
import com.morpheusdata.model.projection.NetworkDomainIdentityProjection
import com.morpheusdata.model.projection.NetworkDomainRecordIdentityProjection
import com.morpheusdata.model.projection.NetworkPoolIdentityProjection
import com.morpheusdata.model.projection.NetworkPoolIpIdentityProjection
import com.morpheusdata.response.ServiceResponse
import groovy.json.JsonSlurper
import groovy.text.SimpleTemplateEngine
import groovy.json.JsonOutput
import groovy.util.logging.Slf4j
import org.apache.commons.net.util.SubnetUtils
import io.reactivex.Completable
import io.reactivex.Single
import io.reactivex.schedulers.Schedulers
import org.apache.http.entity.ContentType
import io.reactivex.Observable
import org.apache.commons.validator.routines.InetAddressValidator

@Slf4j
class MicetroProvider implements IPAMProvider, DNSProvider {
	MorpheusContext morpheusContext
	Plugin plugin
    static String platformUrl = 'mmws/api/v2/'

	static String LOCK_NAME = 'micetro.ipam'
	private java.lang.Object maxResults

	MicetroProvider(Plugin plugin, MorpheusContext morpheusContext) {
		this.morpheusContext = morpheusContext
		this.plugin = plugin
	}

	/**
	 * Returns the Morpheus Context for interacting with data stored in the Main Morpheus Application
	 *
	 * @return an implementation of the MorpheusContext for running Future based rxJava queries
	 */
	@Override
	MorpheusContext getMorpheus() {
		return morpheusContext
	}

	/**
	 * Returns the instance of the Plugin class that this provider is loaded from
	 * @return Plugin class contains references to other providers
	 */
	@Override
	Plugin getPlugin() {
		return plugin
	}

	/**
	 * A unique shortcode used for referencing the provided provider. Make sure this is going to be unique as any data
	 * that is seeded or generated related to this provider will reference it by this code.
	 * @return short code string that should be unique across all other plugin implementations.
	 */
	@Override
	String getCode() {
		return 'micetro'
	}

	/**
	 * Provides the provider name for reference when adding to the Morpheus Orchestrator
	 * NOTE: This may be useful to set as an i18n key for UI reference and localization support.
	 *
	 * @return either an English name of a Provider or an i18n based key that can be scanned for in a properties file.
	 */
	@Override
	String getName() {
		return 'Micetro'
	}

	/**
	 * Validation Method used to validate all inputs applied to the integration of an IPAM Provider upon save.
	 * If an input fails validation or authentication information cannot be verified, Error messages should be returned
	 * via a {@link ServiceResponse} object where the key on the error is the field name and the value is the error message.
	 * If the error is a generic authentication error or unknown error, a standard message can also be sent back in the response.
	 *
	 * @param poolServer The Integration Object contains all the saved information regarding configuration of the IPAM Provider.
	 * @return A response is returned depending on if the inputs are valid or not.
	 */
	@Override
	ServiceResponse verifyNetworkPoolServer(NetworkPoolServer poolServer, Map opts) {
		ServiceResponse<NetworkPoolServer> rtn = ServiceResponse.error()
		rtn.errors = [:]
        if(!poolServer.name || poolServer.name == ''){
            rtn.errors['name'] = 'name is required'
        }
		if(!poolServer.serviceUrl || poolServer.serviceUrl == ''){
			rtn.errors['serviceUrl'] = 'Micetro API URL is required'
		}
		if((!poolServer.serviceUsername || poolServer.serviceUsername == '') && (!poolServer.credentialData?.username || poolServer.credentialData?.username == '')){
			rtn.errors['serviceUsername'] = 'username is required'
		}
		if((!poolServer.servicePassword || poolServer.servicePassword == '') && (!poolServer.credentialData?.password || poolServer.credentialData?.password == '')){
			rtn.errors['servicePassword'] = 'password is required'
		}

		rtn.data = poolServer
		if(rtn.errors.size() > 0){
			rtn.success = false
			return rtn //
		}
        def rpcConfig = getRpcConfig(poolServer)
		HttpApiClient micetroClient = new HttpApiClient()
		try {
			def apiUrl = cleanServiceUrl(poolServer.serviceUrl)
			boolean hostOnline = false
			try {
				def apiUrlObj = new URL(apiUrl)
				def apiHost = apiUrlObj.host
				def apiPort = apiUrlObj.port > 0 ? apiUrlObj.port : (apiUrlObj?.protocol?.toLowerCase() == 'https' ? 443 : 80)
				hostOnline = ConnectionUtils.testHostConnectivity(apiHost, apiPort, true, true, null)
			} catch(e) {
				log.error("Error parsing URL {}", apiUrl, e)
			}
			if(hostOnline) {
				opts.doPaging = false
				opts.maxResults = 1
                def networkList = listNetworks(micetroClient,poolServer, opts)
                    if(networkList.success) {
                        rtn.success = true
                    } else {
                        rtn.msg = networkList.msg ?: 'Error connecting to Micetro'
                    }
            } else {
                rtn.msg = 'Micetro Host Not Reachable'
            }
		} catch(e) {
			log.error("verifyPoolServer error: ${e}", e)
		} finally {
			micetroClient.shutdownClient()
		}
		return rtn
	}

	ServiceResponse<NetworkPoolServer> initializeNetworkPoolServer(NetworkPoolServer poolServer, Map opts) {
		log.info("initializeNetworkPoolServer: ${poolServer.dump()}")
		def rtn = new ServiceResponse()
        try {
            if(poolServer) {
                refresh(poolServer)
                rtn.data = poolServer
            } else {
                rtn.error = 'No pool server found'
            }
        } catch(e) {
            rtn.error = "initializeNetworkPoolServer error: ${e}"
            log.error("initializeNetworkPoolServer error: ${e}", e)
        }
        return rtn
    }

	@Override
	ServiceResponse createNetworkPoolServer(NetworkPoolServer poolServer, Map opts) {
		log.info "createNetworkPoolServer() no-op"
		return ServiceResponse.success() // no-op
	}

	@Override
	ServiceResponse updateNetworkPoolServer(NetworkPoolServer poolServer, Map opts) {
		return ServiceResponse.success() // no-op
	}

	protected ServiceResponse refreshNetworkPoolServer(NetworkPoolServer poolServer, Map opts) {
		def rtn = new ServiceResponse()
        def rpcConfig = getRpcConfig(poolServer)
		log.debug("refreshNetworkPoolServer: {}", poolServer.dump())
		HttpApiClient micetroClient = new HttpApiClient()
		micetroClient.throttleRate = poolServer.serviceThrottleRate
		try {
			def apiUrl = cleanServiceUrl(poolServer.serviceUrl)
			def apiUrlObj = new URL(apiUrl)
			def apiHost = apiUrlObj.host
			def apiPort = apiUrlObj.port > 0 ? apiUrlObj.port : (apiUrlObj?.protocol?.toLowerCase() == 'https' ? 443 : 80)
			def hostOnline = ConnectionUtils.testHostConnectivity(apiHost, apiPort, true, true, null)

			log.debug("online: {} - {}", apiHost, hostOnline)

			def testResults
			// Promise
			if(hostOnline) {
                testResults = testNetworkPoolServer(micetroClient,poolServer) as ServiceResponse<Map>
                if(!testResults.success) {
                    morpheus.network.updateNetworkPoolServerStatus(poolServer, AccountIntegration.Status.error, 'error calling Micetro').blockingGet()
                } else {
                    morpheus.network.updateNetworkPoolServerStatus(poolServer, AccountIntegration.Status.syncing).blockingGet()
                }
            } else {
                morpheus.network.updateNetworkPoolServerStatus(poolServer, AccountIntegration.Status.error, 'Micetro api not reachable')
            }
			Date now = new Date()

            cacheNetworks(micetroClient,poolServer)
            cacheZones(micetroClient,poolServer, opts)
            if(poolServer?.configMap?.inventoryExisting) {
                cacheIpAddressRecords(micetroClient,poolServer, opts)
                cacheZoneRecords(micetroClient,poolServer, opts)
            }
            log.info("Sync Completed in ${new Date().time - now.time}ms")
            morpheus.network.updateNetworkPoolServerStatus(poolServer, AccountIntegration.Status.ok).subscribe().dispose()

			return testResults
		} catch(e) {
			log.error("refreshNetworkPoolServer error: ${e}", e)
		} finally {
			micetroClient.shutdownClient()
		}
		return rtn
	}

	// cacheNetworks methods
	void cacheNetworks(HttpApiClient client, NetworkPoolServer poolServer, Map opts = [:]) {
		opts.doPaging = true
		def listResults = listNetworks(client, poolServer, opts)

		if(listResults.success) {
			List apiItems = listResults.data as List<Map>
			Observable<NetworkPoolIdentityProjection> poolRecords = morpheus.network.pool.listIdentityProjections(poolServer.id)
			SyncTask<NetworkPoolIdentityProjection,Map,NetworkPool> syncTask = new SyncTask(poolRecords, apiItems as Collection<Map>)
			syncTask.addMatchFunction { NetworkPoolIdentityProjection domainObject, Map apiItem ->
				domainObject.externalId == "${apiItem?.ref?.tokenize('/')[1]}"
			}.onDelete {removeItems ->
				morpheus.network.pool.remove(poolServer.id, removeItems).blockingGet()
			}.onAdd { itemsToAdd ->
				addMissingPools(poolServer, itemsToAdd)
			}.withLoadObjectDetails { List<SyncTask.UpdateItemDto<NetworkPoolIdentityProjection,Map>> updateItems ->

				Map<Long, SyncTask.UpdateItemDto<NetworkPoolIdentityProjection, Map>> updateItemMap = updateItems.collectEntries { [(it.existingItem.id): it]}
				return morpheus.network.pool.listById(updateItems.collect{it.existingItem.id} as Collection<Long>).map { NetworkPool pool ->
					SyncTask.UpdateItemDto<NetworkPoolIdentityProjection, Map> matchItem = updateItemMap[pool.id]
					return new SyncTask.UpdateItem<NetworkPool,Map>(existingItem:pool, masterItem:matchItem.masterItem)
				}

			}.onUpdate { List<SyncTask.UpdateItem<NetworkPool,Map>> updateItems ->
				updateMatchedPools(poolServer, updateItems)
			}.start()
		}
	}

	void addMissingPools(NetworkPoolServer poolServer, Collection<Map> chunkedAddList) {
        HttpApiClient client = new HttpApiClient();
        def rpcConfig = getRpcConfig(poolServer)
        HttpApiClient.RequestOptions requestOptions = new HttpApiClient.RequestOptions(ignoreSSL: rpcConfig.ignoreSSL)
		def poolType = new NetworkPoolType(code: 'micetro')
		def poolTypeIpv6 = new NetworkPoolType(code: 'micetroipv6')
		List<NetworkPool> missingPoolsList = []
		chunkedAddList?.each { Map it ->
            def isPool = it.utilizationPercentage
            if ((it?.utilizationPercentage >= 0) || (!it.isContainer && it.name.contains(':'))) {
                def id = it?.ref?.tokenize('/')[1]
                def newNetworkPool
                def name = it.customProperties.Title ?: it.name
                def cidr = it.name
                def startAddress = it.from
                def endAddress = it.to
                def rangeConfig
                def addRange

                if(!startAddress.contains(':')) {
                    def networkInfo = getNetworkPoolConfig(cidr)

                    def addConfig = [account:poolServer.account, poolServer:poolServer, owner:poolServer.account, name:name, externalId:"${id}",
                                    cidr: cidr, type: poolType, poolEnabled:true, parentType:'NetworkPoolServer', parentId:poolServer.id]
                    addConfig += networkInfo.config
                    newNetworkPool = new NetworkPool(addConfig)
                    newNetworkPool.ipRanges = []
                    networkInfo.ranges?.each { range ->
                        rangeConfig = [startAddress:range.startAddress, endAddress:range.endAddress, addressCount:addConfig.ipCount]
                        addRange = new NetworkPoolRange(rangeConfig)
                        newNetworkPool.ipRanges.add(addRange)
                    }
                }

                if(startAddress.contains(':')) {
                    def addConfig = [account:poolServer.account, poolServer:poolServer, owner:poolServer.account, name:name, externalId:"${id}",
                                    cidr: cidr, type: poolTypeIpv6, poolEnabled:true, parentType:'NetworkPoolServer', parentId:poolServer.id]
                    newNetworkPool = new NetworkPool(addConfig)
                    newNetworkPool.ipRanges = []
                    rangeConfig = [cidrIPv6: cidr, startIPv6Address: startAddress, endIPv6Address: endAddress]
                    addRange = new NetworkPoolRange(rangeConfig)
                    newNetworkPool.ipRanges.add(addRange)
                }
                missingPoolsList.add(newNetworkPool)
            }
        }
		morpheus.network.pool.create(poolServer.id, missingPoolsList).blockingGet()
	}

	void updateMatchedPools(NetworkPoolServer poolServer, List<SyncTask.UpdateItem<NetworkPool,Map>> chunkedUpdateList) {
        HttpApiClient client = new HttpApiClient();
        def rpcConfig = getRpcConfig(poolServer)
        HttpApiClient.RequestOptions requestOptions = new HttpApiClient.RequestOptions(ignoreSSL: rpcConfig.ignoreSSL)
        
		List<NetworkPool> poolsToUpdate = []
		chunkedUpdateList?.each { update ->
			NetworkPool existingItem = update.existingItem
            Map network = update.masterItem

			if(existingItem) {
				//update view ?
				def save = false
				def networkIp = network.name
				def displayName = network.customProperties.Title ?: network.name
				
                if(existingItem?.displayName != displayName) {
					existingItem.displayName = displayName
					save = true
				}
				if(existingItem?.cidr != networkIp) {
					existingItem.cidr = networkIp
					save = true
				}
                if(save) {
                    poolsToUpdate << existingItem
                }
            }
        }
		if(poolsToUpdate.size() > 0) {
			morpheus.network.pool.save(poolsToUpdate).blockingGet()
		}
	}

    // Cache Zones methods
	def cacheZones(HttpApiClient client, NetworkPoolServer poolServer, Map opts = [:]) {
		try {
			def listResults = listZones(client, poolServer, opts)

			log.info("listZoneResults: {}", listResults)
			if (listResults.success && listResults.data != null) {
				List apiItems = listResults.data as List<Map>
				Observable<NetworkDomainIdentityProjection> domainRecords = morpheus.network.domain.listIdentityProjections(poolServer.integration.id)

				SyncTask<NetworkDomainIdentityProjection,Map,NetworkDomain> syncTask = new SyncTask(domainRecords, apiItems as Collection<Map>)
				syncTask.addMatchFunction { NetworkDomainIdentityProjection domainObject, Map apiItem ->
					domainObject.externalId == "${apiItem.ref.tokenize('/')[1]}"
				}.addMatchFunction { NetworkDomainIdentityProjection domainObject, Map apiItem ->
					domainObject.name == apiItem.name
				}.onDelete {removeItems ->
					morpheus.network.domain.remove(poolServer.integration.id, removeItems).blockingGet()
				}.onAdd { itemsToAdd ->
					addMissingZones(poolServer, itemsToAdd)
				}.withLoadObjectDetails { List<SyncTask.UpdateItemDto<NetworkDomainIdentityProjection,Map>> updateItems ->
					Map<Long, SyncTask.UpdateItemDto<NetworkDomainIdentityProjection, Map>> updateItemMap = updateItems.collectEntries { [(it.existingItem.id): it]}
					return morpheus.network.domain.listById(updateItems.collect{it.existingItem.id} as Collection<Long>).map { NetworkDomain networkDomain ->
						SyncTask.UpdateItemDto<NetworkDomainIdentityProjection, Map> matchItem = updateItemMap[networkDomain.id]
						return new SyncTask.UpdateItem<NetworkDomain,Map>(existingItem:networkDomain, masterItem:matchItem.masterItem)
					}
				}.onUpdate { List<SyncTask.UpdateItem<NetworkDomain,Map>> updateItems ->
					updateMatchedZones(poolServer, updateItems)
				}.start()
			} 
		} catch (e) {
			log.error("cacheZones error: ${e}", e)
		}
	}
	
    private ServiceResponse listZones(HttpApiClient client, NetworkPoolServer poolServer, Map opts = [:]) {
        def rtn = new ServiceResponse()
        rtn.data = [] // Initialize rtn.data as an empty list
        try {
            def rpcConfig = getRpcConfig(poolServer)
            def apiUrl = cleanServiceUrl(rpcConfig.serviceUrl)
            def apiPath = getServicePath(rpcConfig.serviceUrl) + platformUrl + 'dnsZones'
            def hasMore = true
            def attempt = 0
            def doPaging = opts.doPaging != null ? opts.doPaging : true
            def start = 0
            def maxResults = opts.maxResults ?: 1000

            log.debug("url: ${apiUrl} path: ${apiPath}")

            if(doPaging == true) {
                
                while(hasMore && attempt < 1000) {
                    attempt++
                    HttpApiClient.RequestOptions requestOptions = new HttpApiClient.RequestOptions(ignoreSSL: rpcConfig.ignoreSSL)
                    requestOptions.queryParams = [limit:maxResults.toString(),offset:start.toString()]

                    def results = client.callJsonApi(apiUrl,apiPath,rpcConfig.username,rpcConfig.password,requestOptions,'GET')

                    if(results?.success && results?.error != true) {
                        rtn.success = true
                        if(results.data?.result?.dnsZones?.size() > 0) {
                            rtn.data += results.data.result.dnsZones

                            if(doPaging == true) {
                                start += maxResults
                                hasMore = true
                            } else {
                                hasMore = false
                            }

                        } else {
                            hasMore = false
                        }
                    } else {
                        hasMore = false

                        if(!rtn.success) {
                            rtn.msg = results.error
                        }
                    }
                }
            } else {
                HttpApiClient.RequestOptions requestOptions = new HttpApiClient.RequestOptions(ignoreSSL: rpcConfig.ignoreSSL)
                requestOptions.queryParams = [limit:maxResults.toString(),offset:start.toString()]

                def results = client.callJsonApi(apiUrl,apiPath,rpcConfig.username,rpcConfig.password,requestOptions,'GET')

                if(results?.success && results?.error != true) {
                    rtn.success = true
                    if(results.data?.result?.dnsZones?.size() > 0) {
                        rtn.data = results.data.result.dnsZones
                    }
                } else {
                    if(!rtn.success) {
                        rtn.msg = results.error
                    }
                }
            }
        } catch(e) {
            log.error("listZones error: ${e}", e)
        }

        log.debug("List Zones Results: ${rtn}")
        return rtn
    }

    void addMissingZones(NetworkPoolServer poolServer, Collection addList) {
		List<NetworkDomain> missingZonesList = addList?.collect { Map add ->
			NetworkDomain networkDomain = new NetworkDomain()
			networkDomain.externalId = add.ref.tokenize('/')[1]
			networkDomain.name = NetworkUtility.getFriendlyDomainName(add.name as String)
			networkDomain.fqdn = NetworkUtility.getFqdnDomainName(add.name as String)
			networkDomain.refSource = 'integration'
			networkDomain.zoneType = add.type
			return networkDomain
		}
		log.info("Adding Missing Zone Records! ${missingZonesList}")
		morpheus.network.domain.create(poolServer.integration.id, missingZonesList).blockingGet()
	}

    void updateMatchedZones(NetworkPoolServer poolServer, List<SyncTask.UpdateItem<NetworkDomain,Map>> updateList) {
		def domainsToUpdate = []
		for(SyncTask.UpdateItem<NetworkDomain,Map> update in updateList) {
			NetworkDomain existingItem = update.existingItem as NetworkDomain
			if(existingItem) {
				Boolean save = false
				if(!existingItem.externalId) {
					existingItem.externalId = update.masterItem.ref.tokenize('/')[1]
					save = true
				}
				if(!existingItem.refId) {
					existingItem.refType = update.masterItem.type
					existingItem.refId = poolServer.integration.id
					existingItem.refSource = 'integration'
					save = true
				}

				if(save) {
					domainsToUpdate.add(existingItem)
				}
			}
		}
		if(domainsToUpdate.size() > 0) {
			morpheus.network.domain.save(domainsToUpdate).blockingGet()
		}
	}

    def cacheZoneRecords(HttpApiClient client, NetworkPoolServer poolServer, Map opts) {
		morpheus.network.domain.listIdentityProjections(poolServer.integration.id).flatMap {NetworkDomainIdentityProjection domain ->
			Completable.mergeArray(cacheZoneDomainRecords(client,poolServer,domain,'A',opts),
				cacheZoneDomainRecords(client,poolServer, domain, 'AAAA', opts),
				cacheZoneDomainRecords(client,poolServer, domain, 'PTR', opts),
				cacheZoneDomainRecords(client,poolServer, domain, 'TXT', opts),
				cacheZoneDomainRecords(client,poolServer, domain, 'CNAME', opts),
				cacheZoneDomainRecords(client, poolServer, domain, 'MX', opts)
			).toObservable().subscribeOn(Schedulers.io())
		}.doOnError{ e ->
			log.error("cacheZoneRecords error: ${e}", e)
		}.subscribe()
	}  

	Completable cacheZoneDomainRecords(HttpApiClient client, NetworkPoolServer poolServer, NetworkDomainIdentityProjection domain, String recordType, Map opts) {
		log.info "cacheZoneDomainRecords $poolServer, $domain, $recordType, $opts"
		def listResults = listZoneRecords(client, poolServer, domain, recordType, opts)
		log.debug("listResults: {}",listResults)
		if(listResults.success) {
			List<Map> apiItems = listResults.data as List<Map>
			Observable<NetworkDomainRecordIdentityProjection> domainRecords = morpheus.network.domain.record.listIdentityProjections(domain,recordType)
			SyncTask<NetworkDomainRecordIdentityProjection,Map,NetworkDomainRecord> syncTask = new SyncTask(domainRecords, apiItems as Collection<Map>)
			syncTask.addMatchFunction { NetworkDomainRecordIdentityProjection domainObject, Map apiItem ->
				domainObject.externalId == apiItem.ref.tokenize('/')[1]
			}.onDelete {removeItems ->
				morpheus.network.domain.record.remove(domain, removeItems).blockingGet()
			}.onAdd { itemsToAdd ->
				addMissingDomainRecords(domain, recordType, itemsToAdd)
			}.withLoadObjectDetails { List<SyncTask.UpdateItemDto<NetworkDomainRecordIdentityProjection,Map>> updateItems ->

				Map<Long, SyncTask.UpdateItemDto<NetworkDomainRecordIdentityProjection, Map>> updateItemMap = updateItems.collectEntries { [(it.existingItem.id): it]}
				return morpheus.network.domain.record.listById(updateItems.collect{it.existingItem.id} as Collection<Long>).map { NetworkDomainRecord networkDomainRecord ->
					SyncTask.UpdateItemDto<NetworkDomainRecordIdentityProjection, Map> matchItem = updateItemMap[networkDomainRecord.id]
					return new SyncTask.UpdateItem<NetworkDomainRecord,Map>(existingItem:networkDomainRecord, masterItem:matchItem.masterItem)
				}


			}.onUpdate { List<SyncTask.UpdateItem<NetworkPool,Map>> updateItems ->
				updateMatchedDomainRecords(recordType,domain, updateItems)
			}
			Completable.fromObservable(syncTask.observe())
		} else {
			return Completable.complete()
		}
	}

    void addMissingDomainRecords(NetworkDomainIdentityProjection domain, String recordType, Collection<Map> addList) {
		List<NetworkDomainRecord> records = []
		addList?.each {            
            if (it.name) {
                def addConfig = [networkDomain: new NetworkDomain(id: domain.id), externalId:it.ref.tokenize('/')[1],ttl: it.ttl.toInteger(), name: it.name, content: it.data, fqdn: "${it.name}.${domain.name ?: ""}", type: recordType, source: 'sync']

                def newObj = new NetworkDomainRecord(addConfig)
                records.add(newObj)
            }
		}
		morpheus.network.domain.record.create(domain,records).blockingGet()
	}

    void updateMatchedDomainRecords(String recordType,domain,List<SyncTask.UpdateItem<NetworkDomainRecord, Map>> updateList) {
		def records = []
		updateList?.each { update ->
			NetworkDomainRecord existingItem = update.existingItem
			if(existingItem) {
				//update view ?
				def save = false
                def name = update.masterItem.name
                def fqdn = "${name}.${domain.name ?: ""}"
                def data = update.masterItem.data
                def ttl = update.masterItem.ttl.toInteger()

                if (existingItem.name != name) {
                    existingItem.name = name
                    save = true
                }

                if (existingItem.fqdn != fqdn) {
                    existingItem.fqdn = fqdn
                    save = true
                }

                if (existingItem.content != data) {
                    existingItem.content = data
                    save = true
                }

                if (existingItem.ttl != ttl) {
                    existingItem.ttl = ttl
                    save = true
                }

				if(save) {
					records.add(existingItem)
				}
			}
		}
		if(records.size() > 0) {
			morpheus.network.domain.record.save(records).blockingGet()
		}
	}

    ServiceResponse listZoneRecords(HttpApiClient client, NetworkPoolServer poolServer, NetworkDomainIdentityProjection domain, String recordType = 'A', Map opts=[:]) {
        def rtn = new ServiceResponse()
        rtn.data = [] // Initialize rtn.data as an empty list
        try {
            def rpcConfig = getRpcConfig(poolServer)
            def apiUrl = cleanServiceUrl(rpcConfig.serviceUrl)
            def apiPath = getServicePath(rpcConfig.serviceUrl) + platformUrl + 'dnsZones/' + domain.externalId + '/dnsRecords'
            def hasMore = true
            def attempt = 0
            def doPaging = opts.doPaging != null ? opts.doPaging : true
            def start = 0
            def maxResults = opts.maxResults ?: 1000

            log.debug("url: ${apiUrl} path: ${apiPath}")

            if(doPaging == true) {
                
                while(hasMore && attempt < 1000) {
                    attempt++
                    HttpApiClient.RequestOptions requestOptions = new HttpApiClient.RequestOptions(ignoreSSL: rpcConfig.ignoreSSL)
                    requestOptions.queryParams = ["filter":"type=${recordType}",limit:maxResults.toString(),offset:start.toString()]

                    def results = client.callJsonApi(apiUrl,apiPath,rpcConfig.username,rpcConfig.password,requestOptions,'GET')

                    if(results?.success && results?.error != true) {
                        rtn.success = true
                        if(results.data?.result?.dnsRecords?.size() > 0) {
                            rtn.data += results.data.result.dnsRecords

                            if(doPaging == true) {
                                start += maxResults
                                hasMore = true
                            } else {
                                hasMore = false
                            }

                        } else {
                            hasMore = false
                        }
                    } else {
                        hasMore = false

                        if(!rtn.success) {
                            rtn.msg = results.error
                        }
                    }
                }
                
            } else {
                HttpApiClient.RequestOptions requestOptions = new HttpApiClient.RequestOptions(ignoreSSL: rpcConfig.ignoreSSL)
                requestOptions.queryParams = [limit:maxResults.toString(),offset:start.toString()]

                def results = client.callJsonApi(apiUrl,apiPath,rpcConfig.username,rpcConfig.password,requestOptions,'GET')

                if(results?.success && results?.error != true) {
                    rtn.success = true
                    if(results.data?.result?.dnsRecords?.size() > 0) {
                        rtn.data = results.data.result.dnsRecords
                    }
                } else {
                    if(!rtn.success) {
                        rtn.msg = results.error
                    }
                }
            }
        } catch(e) {
            log.error("listZoneRecords error: ${e}", e)
        }

        log.debug("List Zone Records Results: ${rtn}")
        return rtn
    }

    @Override
	ServiceResponse<NetworkDomainRecord> createRecord(AccountIntegration integration, NetworkDomainRecord record, Map opts) {
        ServiceResponse<NetworkDomainRecord> rtn = new ServiceResponse<>()
		HttpApiClient client = new HttpApiClient()
		def poolServer = morpheus.network.getPoolServerByAccountIntegration(integration).blockingGet()

        try {
			if(integration) {

				def fqdn = record.name
				if(record.name.endsWith(record.networkDomain.name)) {
					fqdn = record.name.tokenize('.')[0]
				}

                def rpcConfig = getRpcConfig(poolServer)
                HttpApiClient.RequestOptions requestOptions = new HttpApiClient.RequestOptions(ignoreSSL: rpcConfig.ignoreSSL)
				def apiUrl = cleanServiceUrl(rpcConfig.serviceUrl)
				def recordType = record.type
				def apiPath = platformUrl + 'dnsZones/' + record.networkDomain.externalId.toString() + '/dnsRecords'
				def results = new ServiceResponse()
				requestOptions.body = JsonOutput.toJson(['dnsRecord':['ttl':"${record.ttl ?: 3600}",'data':record.content,'type':recordType,'name':fqdn]])

                results = client.callJsonApi(apiUrl,apiPath,rpcConfig.username,rpcConfig.password,requestOptions,'POST')

                if(results.success && !results.error) {
                    record.externalId = results.data.result?.ref.tokenize('/')[1]
                    return new ServiceResponse<NetworkDomainRecord>(true,null,null,record)
                    rtn.data = record
                    rtn.success = true
                }
			} else {
            log.warn("no integration")
            }   
		} catch(e) {
			log.error("provisionServer error: ${e}", e)
		} finally {
			client.shutdownClient()
		}
		return rtn
	}

    ServiceResponse deleteRecord(AccountIntegration integration, NetworkDomainRecord record, Map opts) {
        def rtn = new ServiceResponse()
        try {
            if(integration) {
                morpheus.network.getPoolServerByAccountIntegration(integration).doOnSuccess({ poolServer ->
                    def rpcConfig = getRpcConfig(poolServer)
                    HttpApiClient client = new HttpApiClient()
                    HttpApiClient.RequestOptions requestOptions = new HttpApiClient.RequestOptions(ignoreSSL: rpcConfig.ignoreSSL)
                    def apiUrl = cleanServiceUrl(rpcConfig.serviceUrl)
                    try {
                        def apiPath = platformUrl + 'dnsRecords/' + record.externalId

                        def results = client.callApi(apiUrl, apiPath, rpcConfig.username, rpcConfig.password, requestOptions, 'DELETE')
                        log.info("deleteRecord results: ${results}")
                        if(results.success) {
							rtn.success = true
						}
                    } finally {
						client.shutdownClient()
					}
                }).doOnError({error ->
					log.error("Error deleting record: {}",error.message,error)
				}).doOnSubscribe({ sub ->
					log.info "Subscribed"
				}).blockingGet()
				return ServiceResponse.success()
			} else {
			log.warn("no integration")
			}
        } catch(e) {
            log.error("provisionServer error: ${e}", e)
        }
        return rtn
    }

	@Override
	ServiceResponse createHostRecord(NetworkPoolServer poolServer, NetworkPool networkPool, NetworkPoolIp networkPoolIp, NetworkDomain domain, Boolean createARecord, Boolean createPtrRecord) {
		HttpApiClient client = new HttpApiClient();
        InetAddressValidator inetAddressValidator = new InetAddressValidator()
        
        def rpcConfig = getRpcConfig(poolServer)
        def results = new ServiceResponse()
        def apiUrl = cleanServiceUrl(rpcConfig.serviceUrl)
        def apiPath

        HttpApiClient.RequestOptions requestOptions = new HttpApiClient.RequestOptions(ignoreSSL: rpcConfig.ignoreSSL)

        try {
            def hostname = networkPoolIp.hostname
            def customProperty = poolServer.configMap?.nameProperty
            def recordType

            if(domain && hostname && !hostname.endsWith(domain.name))  {
                hostname = "${hostname}.${domain.name}"
            }

            // Check if Static IP Was Asked
            if (networkPoolIp.ipAddress) {
                // Make sure it's a valid IP
                if (inetAddressValidator.isValidInet4Address(networkPoolIp.ipAddress)) {
                    log.info("A Valid IPv4 Address Entered: ${networkPoolIp.ipAddress}")
                } else if (inetAddressValidator.isValidInet6Address(networkPoolIp.ipAddress)) {
                    log.info("A Valid IPv6 Address Entered: ${networkPoolIp.ipAddress}")
                } else {
                    log.error("Invalid IP Address Requested: ${networkPoolIp.ipAddress}", results)
                    return ServiceResponse.error("Invalid IP Address Requested: ${networkPoolIp.ipAddress}")
                }
            } else {
                // Grab next available IP
                
                apiPath = getServicePath(rpcConfig.serviceUrl) + platformUrl + 'ranges/' + networkPool.externalId.toString() + '/nextFreeAddress'
                requestOptions.queryParams = ['temporaryClaimTime':'120']

                results = client.callJsonApi(apiUrl,apiPath,rpcConfig.username,rpcConfig.password,requestOptions,'GET')

                if (results?.success) {
                networkPoolIp.ipAddress = results?.data?.result?.address
                } else {
                    log.warn("API Call Failed to allocate IP Address")
                    return ServiceResponse.error("API Call Failed to allocate IP Address",null,networkPoolIp)
                }
            }

            if (networkPool.type.code == 'micetro'){
                recordType = 'A'
            } else {
                recordType = 'AAAA'
            }

            if(createARecord) {
                networkPoolIp.domain = domain
            }
            if (networkPoolIp.id) {
                networkPoolIp = morpheus.network.pool.poolIp.save(networkPoolIp)?.blockingGet()
            } else {
                networkPoolIp = morpheus.network.pool.poolIp.create(networkPoolIp)?.blockingGet()
            }

            if (createARecord && domain && domain.refId == poolServer.integration.id) {
                log.info("Attempting DNS record...")
                def fqdn = hostname
				if(hostname.endsWith(domain.name)) {
					fqdn = hostname.tokenize('.')[0]
				}

				apiPath = platformUrl + 'dnsZones/' + domain.externalId.toString() + '/dnsRecords'
                requestOptions.queryParams = [:]
				requestOptions.body = JsonOutput.toJson(['dnsRecord':['ttl':'3600','data':networkPoolIp.ipAddress,'type':recordType,'name':fqdn]])

                results = client.callJsonApi(apiUrl,apiPath,rpcConfig.username,rpcConfig.password,requestOptions,'POST')
            } else {
                log.info("Attempting IPAM record...")
                apiPath = getServicePath(rpcConfig.serviceUrl) + platformUrl + 'ipamRecords/ref'
                requestOptions.queryParams = [:]
                requestOptions.body = JsonOutput.toJson(['ref':networkPoolIp.ipAddress,'objType':'IPAddress','saveComment':'Created with Morpheus','properties':[(customProperty):hostname]])

                results = client.callJsonApi(apiUrl,apiPath,rpcConfig.username,rpcConfig.password,requestOptions,'PUT')
            }

            if (results.success && !results.error) {
                networkPoolIp.externalId = networkPoolIp.ipAddress
                return ServiceResponse.success(networkPoolIp)
            } else {
                log.warn("API Call Failed to allocate IP Address")
                return ServiceResponse.error("API Call Failed to allocate IP Address",null,networkPoolIp)
            }
        } catch(e) {
            log.warn("API Call Failed to allocate IP Address {}",e)
            return ServiceResponse.error("API Call Failed to allocate IP Address",null,networkPoolIp)
        } finally {
            client.shutdownClient()
        }
	}

	@Override
	ServiceResponse updateHostRecord(NetworkPoolServer poolServer, NetworkPool networkPool, NetworkPoolIp networkPoolIp) {
		HttpApiClient client = new HttpApiClient();
        def rpcConfig = getRpcConfig(poolServer)
        HttpApiClient.RequestOptions requestOptions = new HttpApiClient.RequestOptions(ignoreSSL: rpcConfig.ignoreSSL)

        try {
            def results = []
            def hostname = networkPoolIp.hostname
            def customProperty = poolServer.configMap?.nameProperty
            def apiUrl = cleanServiceUrl(rpcConfig.serviceUrl)
            def apiPath = getServicePath(rpcConfig.serviceUrl) + platformUrl + 'ipamRecords/ref'
            requestOptions.queryParams = [:]
            requestOptions.body = JsonOutput.toJson(['ref':networkPoolIp.ipAddress,'objType':'IPAddress','saveComment':'Created with Morpheus','properties':[(customProperty):hostname]])

            results = client.callJsonApi(apiUrl,apiPath,rpcConfig.username,rpcConfig.password,requestOptions,'PUT')

            if (results.success && !results.error) {
                    return ServiceResponse.success(networkPoolIp)
                } else {
				    return ServiceResponse.error(results.error ?: 'Error Updating Host Record', null, networkPoolIp)
			    }
        } catch(ex) {
            log.error("Error Updating Host Record {}",ex.message,ex)
            return ServiceResponse.error("Error Updating Host Record ${ex.message}",null,networkPoolIp)
        } finally {
            client.shutdownClient()
        }
	}

    @Override
	void refresh(AccountIntegration integration) {
	 //NOOP
	}

    @Override
	ServiceResponse verifyAccountIntegration(AccountIntegration integration, Map opts) {
		//NOOP
		return null
	}

	@Override
	ServiceResponse deleteHostRecord(NetworkPool networkPool, NetworkPoolIp networkPoolIp, Boolean deleteAssociatedRecords ) {
		HttpApiClient client = new HttpApiClient();
        def poolServer = morpheus.network.getPoolServerById(networkPool.poolServer.id).blockingGet()
        def rpcConfig = getRpcConfig(poolServer)
        HttpApiClient.RequestOptions requestOptions = new HttpApiClient.RequestOptions(ignoreSSL: rpcConfig.ignoreSSL)

        try {
            def results = []
            def apiUrl = cleanServiceUrl(rpcConfig.serviceUrl)
            def apiPath = getServicePath(rpcConfig.serviceUrl) + platformUrl + 'ipamRecords/' + networkPoolIp.externalId.toString()

            results = client.callJsonApi(apiUrl,apiPath,rpcConfig.username,rpcConfig.password,requestOptions,'DELETE')

            if(results.success && !results.error) {
                return ServiceResponse.success(networkPoolIp)
            } else {
                log.error("Error Deleting Host Record ${networkPoolIp}")
                return ServiceResponse.error("Error Deleting Host Record ${networkPoolIp}")
            }
        } catch(x) {
            log.error("Error Deleting Host Record {}",x.message,x)
            return ServiceResponse.error("Error Deleting Host Record ${x.message}",null,networkPoolIp)
        } finally {
            client.shutdownClient()
        }
	}

	private ServiceResponse listNetworks(HttpApiClient client, NetworkPoolServer poolServer, Map opts = [:]) {
        def rtn = new ServiceResponse()
        rtn.data = [] // Initialize rtn.data as an empty list
        try {
            def rpcConfig = getRpcConfig(poolServer)
            def apiUrl = cleanServiceUrl(rpcConfig.serviceUrl)
            def apiPath = getServicePath(rpcConfig.serviceUrl) + platformUrl + 'ranges'
            def hasMore = true
            def attempt = 0
            def doPaging = opts.doPaging != null ? opts.doPaging : true
            def start = 0
            def maxResults = opts.maxResults ?: 1000

            log.debug("url: ${apiUrl} path: ${apiPath}")

            if(doPaging == true) {
                
                while(hasMore && attempt < 1000) {
                    attempt++
                    HttpApiClient.RequestOptions requestOptions = new HttpApiClient.RequestOptions(ignoreSSL: rpcConfig.ignoreSSL)
                    requestOptions.queryParams = [limit:maxResults.toString(),offset:start.toString()]

                    def results = client.callJsonApi(apiUrl,apiPath,rpcConfig.username,rpcConfig.password,requestOptions,'GET')

                    if(results?.success && results?.error != true) {
                        rtn.success = true
                        if(results.data?.result?.ranges?.size() > 0) {
                            rtn.data += results.data.result.ranges

                            if(doPaging == true) {
                                start += maxResults
                                hasMore = true
                            } else {
                                hasMore = false
                            }

                        } else {
                            hasMore = false
                        }
                    } else {
                        hasMore = false

                        if(!rtn.success) {
                            rtn.msg = results.error
                        }
                    }
                }
            } else {
                HttpApiClient.RequestOptions requestOptions = new HttpApiClient.RequestOptions(ignoreSSL: rpcConfig.ignoreSSL)
                requestOptions.queryParams = [limit:maxResults.toString(),offset:start.toString()]

                def results = client.callJsonApi(apiUrl,apiPath,rpcConfig.username,rpcConfig.password,requestOptions,'GET')

                if(results?.success && results?.error != true) {
                    rtn.success = true
                    if(results.data?.result?.ranges?.size() > 0) {
                        rtn.data = results.data.result.ranges
                    }
                } else {
                    if(!rtn.success) {
                        rtn.msg = results.error
                    }
                }
            }
        } catch(e) {
            log.error("listNetworks error: ${e}", e)
        }

        log.debug("List Networks Results: ${rtn}")
        return rtn
	}

	// cacheIpAddressRecords
    void cacheIpAddressRecords(HttpApiClient client, NetworkPoolServer poolServer, Map opts=[:]) {
        morpheus.network.pool.listIdentityProjections(poolServer.id).buffer(50).flatMap { Collection<NetworkPoolIdentityProjection> poolIdents ->
            return morpheus.network.pool.listById(poolIdents.collect{it.id})
        }.flatMap { NetworkPool pool ->
            def listResults = listHostRecords(client,poolServer,pool)
            if (listResults.success) {

                List<Map> apiItems = listResults.data
                Observable<NetworkPoolIpIdentityProjection> poolIps = morpheus.network.pool.poolIp.listIdentityProjections(pool.id)
                SyncTask<NetworkPoolIpIdentityProjection, Map, NetworkPoolIp> syncTask = new SyncTask<NetworkPoolIpIdentityProjection, Map, NetworkPoolIp>(poolIps, apiItems)
                return syncTask.addMatchFunction { NetworkPoolIpIdentityProjection ipObject, Map apiItem ->
                    ipObject.externalId == "${apiItem.id}"
                }.addMatchFunction { NetworkPoolIpIdentityProjection domainObject, Map apiItem ->
                    domainObject.ipAddress == apiItem?.address?.tokenize('/')[0]
                }.onDelete {removeItems ->
                    morpheus.network.pool.poolIp.remove(pool.id, removeItems).blockingGet()
                }.onAdd { itemsToAdd ->
                    addMissingIps(pool, itemsToAdd)
                }.withLoadObjectDetails { List<SyncTask.UpdateItemDto<NetworkPoolIpIdentityProjection,Map>> updateItems ->

                    Map<Long, SyncTask.UpdateItemDto<NetworkPoolIpIdentityProjection, Map>> updateItemMap = updateItems.collectEntries { [(it.existingItem.id): it]}
                    return morpheus.network.pool.poolIp.listById(updateItems.collect{it.existingItem.id} as Collection<Long>).map { NetworkPoolIp poolIp ->
                        SyncTask.UpdateItemDto<NetworkPoolIpIdentityProjection, Map> matchItem = updateItemMap[poolIp.id]
                        return new SyncTask.UpdateItem<NetworkPoolIp,Map>(existingItem:poolIp, masterItem:matchItem.masterItem)
                    }

                }.onUpdate { List<SyncTask.UpdateItem<NetworkDomain,Map>> updateItems ->
                    updateMatchedIps(updateItems)
                }.observe()
            } else {
                return Single.just(false)
            }
        }.doOnError{ e ->
            log.error("cacheIpRecords error: ${e}", e)
        }.subscribe()

    }

	void addMissingIps(NetworkPool pool, List addList) {
        List<NetworkPoolIp> poolIpsToAdd = addList?.collect { it ->
            def externalId = it.address
			def ipAddress = it.address
			def types = it.state
            def names = it.dnsHosts?.dnsRecord?.name ?: null
			def ipType = 'assigned'
            if(types == ('Claimed' || 'Held')) {
                ipType = 'reserved'
            }
            if(types == 'Assigned') {
                ipType = 'assigned'
            }
			def addConfig = [networkPool: pool, networkPoolRange: pool.ipRanges ? pool.ipRanges.first() : null, ipType: ipType, hostname: names, ipAddress: ipAddress.toString(), externalId:externalId]
			def newObj = new NetworkPoolIp(addConfig)
			return newObj

		}
		if(poolIpsToAdd.size() > 0) {
			morpheus.network.pool.poolIp.create(pool, poolIpsToAdd).blockingGet()
		}
	}

	void updateMatchedIps(List<SyncTask.UpdateItem<NetworkPoolIp,Map>> updateList) {
		List<NetworkPoolIp> ipsToUpdate = []
		updateList?.each {  update ->
			NetworkPoolIp existingItem = update.existingItem
            // reserved,assigned,unmanaged,transient
			if(existingItem) {
				def hostname = update.masterItem.dnsHosts?.dnsRecord?.name ?: null
                def types = update.masterItem.state
				def ipType = 'assigned'
                if(types == ('Claimed' || 'Held')) {
                    ipType = 'reserved'
                }
                if(types == 'Assigned') {
                    ipType = 'assigned'
                }
				def save = false
				if(existingItem.ipType != ipType) {
					existingItem.ipType = ipType
					save = true
				}
				if(existingItem.hostname != hostname) {
					existingItem.hostname = hostname
					save = true

				}
				if(save) {
					ipsToUpdate << existingItem
				}
			}
		}
		if(ipsToUpdate.size() > 0) {
			morpheus.network.pool.poolIp.save(ipsToUpdate)
		}
	}


	private ServiceResponse listHostRecords(HttpApiClient client, NetworkPoolServer poolServer,NetworkPool networkPool, Map opts = [:]) {
        def rtn = new ServiceResponse()
        rtn.data = [] // Initialize rtn.data as an empty list
        try {
            def rpcConfig = getRpcConfig(poolServer)
            def apiUrl = cleanServiceUrl(rpcConfig.serviceUrl)
            def apiPath = getServicePath(rpcConfig.serviceUrl) + platformUrl + 'ranges/' + networkPool.externalId.toString() + '/ipamRecords'
            def hasMore = true
            def attempt = 0
            def doPaging = opts.doPaging != null ? opts.doPaging : true
            def start = 0
            def maxResults = opts.maxResults ?: 1000

            log.debug("url: ${apiUrl} path: ${apiPath}")

            if(doPaging == true) {
                
                while(hasMore && attempt < 1000) {
                    attempt++
                    HttpApiClient.RequestOptions requestOptions = new HttpApiClient.RequestOptions(ignoreSSL: rpcConfig.ignoreSSL)
                    requestOptions.queryParams = ['filter':'state!=Free',limit:maxResults.toString(),offset:start.toString()]

                    def results = client.callJsonApi(apiUrl,apiPath,rpcConfig.username,rpcConfig.password,requestOptions,'GET')

                    if(results?.success && results?.error != true) {
                        rtn.success = true
                        if(results.data?.result?.ipamRecords?.size() > 0) {
                            rtn.data += results.data.result.ipamRecords

                            if(doPaging == true) {
                                start += maxResults
                                hasMore = true
                            } else {
                                hasMore = false
                            }

                        } else {
                            hasMore = false
                        }
                    } else {
                        hasMore = false

                        if(!rtn.success) {
                            rtn.msg = results.error
                        }
                    }
                }
            } else {
                HttpApiClient.RequestOptions requestOptions = new HttpApiClient.RequestOptions(ignoreSSL: rpcConfig.ignoreSSL)
                requestOptions.queryParams = [limit:maxResults.toString(),offset:start.toString()]

                def results = client.callJsonApi(apiUrl,apiPath,rpcConfig.username,rpcConfig.password,requestOptions,'GET')

                if(results?.success && results?.error != true) {
                    rtn.success = true
                    if(results.data?.result?.ipamRecords?.size() > 0) {
                        rtn.data = results.data.result.ipamRecords
                    }
                } else {
                    if(!rtn.success) {
                        rtn.msg = results.error
                    }
                }
            }
        } catch(e) {
            log.error("listHostRecords error: ${e}", e)
        }

        log.debug("List Host Results: ${rtn}")
        return rtn
	}


	ServiceResponse testNetworkPoolServer(HttpApiClient client, NetworkPoolServer poolServer) {
		def rtn = new ServiceResponse()
		try {
			def opts = [doPaging:false, maxResults:1]
			def networkList = listNetworks(client, poolServer, opts)
			rtn.success = networkList.success
			rtn.data = [:]
			if(!networkList.success) {
				rtn.msg = 'error connecting to Micetro'
			}
		} catch(e) {
			rtn.success = false
			log.error("test network pool server error: ${e}", e)
		}
		return rtn
	}

    /**
     * Periodically called to refresh and sync data coming from the relevant integration. Most integration providers
     * provide a method like this that is called periodically (typically 5 - 10 minutes). DNS Sync operates on a 10min
     * cycle by default. Useful for caching Host Records created outside of Morpheus.
     * @param poolServer The Integration Object contains all the saved information regarding configuration of the IPAM Provider.
     */
    @Override
    void refresh(NetworkPoolServer poolServer) {
        refreshNetworkPoolServer(poolServer, [:])
    }

	/**
	 * An IPAM Provider can register pool types for display and capability information when syncing IPAM Pools
	 * @return a List of {@link NetworkPoolType} to be loaded into the Morpheus database.
	 */
	Collection<NetworkPoolType> getNetworkPoolTypes() {
		return [
			new NetworkPoolType(code:'micetro', name:'Micetro', creatable:false, description:'Micetro', rangeSupportsCidr: false),
			new NetworkPoolType(code:'micetroipv6', name:'Micetro IPv6', creatable:false, description:'Micetro IPv6', rangeSupportsCidr: true, ipv6Pool:true)
		]
	}

	/**
	 * Provide custom configuration options when creating a new {@link AccountIntegration}
	 * @return a List of OptionType
	 */
	@Override
	List<OptionType> getIntegrationOptionTypes() {
		return [
				new OptionType(code: 'micetro.serviceUrl', name: 'Service URL', inputType: OptionType.InputType.TEXT, fieldName: 'serviceUrl', fieldLabel: 'API Url', fieldContext: 'domain', placeHolder: 'https://x.x.x.x/', displayOrder: 0, required:true),
				new OptionType(code: 'micetro.credentials', name: 'Credentials', inputType: OptionType.InputType.CREDENTIAL, fieldName: 'type', fieldLabel: 'Credentials', fieldContext: 'credential', required: true, displayOrder: 1, defaultValue: 'local',optionSource: 'credentials',config: '{"credentialTypes":["username-password"]}'),

				new OptionType(code: 'micetro.serviceUsername', name: 'Service Username', inputType: OptionType.InputType.TEXT, fieldName: 'serviceUsername', fieldLabel: 'Username', fieldContext: 'domain', displayOrder: 2,localCredential: true, required: true),
				new OptionType(code: 'micetro.servicePassword', name: 'Service Password', inputType: OptionType.InputType.PASSWORD, fieldName: 'servicePassword', fieldLabel: 'Password', fieldContext: 'domain', displayOrder: 3,localCredential: true, required: true),
				new OptionType(code: 'micetro.throttleRate', name: 'Throttle Rate', inputType: OptionType.InputType.NUMBER, defaultValue: 0, fieldName: 'serviceThrottleRate', fieldLabel: 'Throttle Rate', fieldContext: 'domain', displayOrder: 4),
				new OptionType(code: 'micetro.ignoreSsl', name: 'Ignore SSL', inputType: OptionType.InputType.CHECKBOX, defaultValue: 0, fieldName: 'ignoreSsl', fieldLabel: 'Disable SSL SNI Verification', fieldContext: 'domain', displayOrder: 5),
				new OptionType(code: 'micetro.inventoryExisting', name: 'Inventory Existing', inputType: OptionType.InputType.CHECKBOX, defaultValue: 0, fieldName: 'inventoryExisting', fieldLabel: 'Inventory Existing', fieldContext: 'config', displayOrder: 6),
                new OptionType(code: 'micetro.nameProperty', name: 'Name Property', inputType: OptionType.InputType.TEXT, fieldName: 'nameProperty', fieldLabel: 'Name Property', fieldContext: 'config', helpBlock: "Supply the Custom Property [Key] that will Store Hostname [Value]", placeHolder: "serverName", displayOrder: 7, required: true)

		]
	}

	@Override
	Icon getIcon() {
		return new Icon(path:"micetro.png", darkPath: "micetro.png")
	}

    private getRpcConfig(NetworkPoolServer poolServer) {
        return [
            username:poolServer.credentialData?.username ?: poolServer.serviceUsername,
            password:poolServer.credentialData?.password ?: poolServer.servicePassword,
            serviceUrl:poolServer.serviceUrl,
            ignoreSSL: poolServer.ignoreSsl
        ]
    }

	private static String cleanServiceUrl(String url) {
		def rtn = url
		def slashIndex = rtn?.indexOf('/', 9)
		if(slashIndex > 9)
			rtn = rtn.substring(0, slashIndex)
		return rtn
	}

	private static String getServicePath(String url) {
		def rtn = '/'
		def slashIndex = url?.indexOf('/', 9)
		if(slashIndex > 9)
			rtn = url.substring(slashIndex)
		if(rtn?.endsWith('/'))
			return rtn.substring(0, rtn.lastIndexOf("/"));
		return rtn
	}

    static Map getNetworkPoolConfig(String cidr) {
        def rtn = [config:[:], ranges:[]]
        try {
            def subnetInfo = new SubnetUtils(cidr).getInfo()
            rtn.config.netmask = subnetInfo.getNetmask()
            rtn.config.ipCount = subnetInfo.getAddressCountLong() ?: 0
            rtn.config.ipFreeCount = rtn.config.ipCount
            rtn.ranges << [startAddress:subnetInfo.getLowAddress(), endAddress:subnetInfo.getHighAddress()]
        } catch(e) {
            log.warn("error parsing network pool cidr: ${e}", e)
        }
        return rtn
    }

}
