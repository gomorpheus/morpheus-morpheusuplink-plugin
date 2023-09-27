package com.morpheusdata.morpheusuplink

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
class MorpheusUplinkProvider implements IPAMProvider, DNSProvider {
	MorpheusContext morpheusContext
	Plugin plugin
    static String authPath = 'oauth/token'
    static String networkPoolsPath = 'api/networks/pools/'
    static String networkDomainsPath = 'api/networks/domains/'

	static String LOCK_NAME = 'morpheusuplink.ipam'
	private java.lang.Object maxResults

	MorpheusUplinkProvider(Plugin plugin, MorpheusContext morpheusContext) {
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
		return 'morpheusuplink'
	}

	/**
	 * Provides the provider name for reference when adding to the Morpheus Orchestrator
	 * NOTE: This may be useful to set as an i18n key for UI reference and localization support.
	 *
	 * @return either an English name of a Provider or an i18n based key that can be scanned for in a properties file.
	 */
	@Override
	String getName() {
		return 'Morpheus Uplink'
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
			rtn.errors['serviceUrl'] = 'Morpheus URL is required'
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
		HttpApiClient morpheusUplinkClient = new HttpApiClient()
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
                def tokenResults = login(morpheusUplinkClient,rpcConfig)
                def networkList
                if(tokenResults.success) {
                    def token = tokenResults.token.toString()
                    networkList = listNetworks(morpheusUplinkClient,token,poolServer,opts)
                    if(networkList.success) {
                        rtn.success = true
                    } else {
                        rtn.msg = networkList.msg ?: 'Error connecting to Morpheus'
                    }
                } else {
                    rtn.msg = tokenResults.msg ?: 'Error connecting to Morpheus'
                }
            } else {
                rtn.msg = 'Morpheus Host Not Reachable'
            }
		} catch(e) {
			log.error("verifyPoolServer error: ${e}", e)
		} finally {
			morpheusUplinkClient.shutdownClient()
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
        def tokenResults
        def token
        def rpcConfig = getRpcConfig(poolServer)
		log.debug("refreshNetworkPoolServer: {}", poolServer.dump())
		HttpApiClient morpheusUplinkClient = new HttpApiClient()
		morpheusUplinkClient.throttleRate = poolServer.serviceThrottleRate
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
                tokenResults = login(morpheusUplinkClient,rpcConfig)
                if(tokenResults.success) {
                token = tokenResults?.token as String
                testResults = testNetworkPoolServer(morpheusUplinkClient,token,poolServer) as ServiceResponse<Map>
                    if(!testResults.success) {
                        morpheus.network.updateNetworkPoolServerStatus(poolServer, AccountIntegration.Status.error, 'error calling Micetro').blockingGet()
                    } else {
                        morpheus.network.updateNetworkPoolServerStatus(poolServer, AccountIntegration.Status.syncing).blockingGet()
                    }
                } else {
                    morpheus.network.updateNetworkPoolServerStatus(poolServer, AccountIntegration.Status.error, 'Morpheus api not reachable')
                    return ServiceResponse.error("Morpheus api not reachable")
                }
            } else {
                morpheus.network.updateNetworkPoolServerStatus(poolServer, AccountIntegration.Status.error, 'Micetro api not reachable')
                return ServiceResponse.error("Morpheus api not reachable")
            }
			Date now = new Date()
            if(testResults?.success) {
                cacheNetworks(morpheusUplinkClient,token,poolServer,opts)
                cacheZones(morpheusUplinkClient,token,poolServer,opts)
                if(poolServer?.configMap?.inventoryExisting) {
                    cacheIpAddressRecords(morpheusUplinkClient,token,poolServer,opts)
                    cacheZoneRecords(morpheusUplinkClient,token,poolServer,opts)
                }
                log.info("Sync Completed in ${new Date().time - now.time}ms")
                morpheus.network.updateNetworkPoolServerStatus(poolServer, AccountIntegration.Status.ok).subscribe().dispose()
            }
			return testResults
		} catch(e) {
			log.error("refreshNetworkPoolServer error: ${e}", e)
		} finally {
			morpheusUplinkClient.shutdownClient()
		}
		return rtn
	}

	// cacheNetworks methods
	void cacheNetworks(HttpApiClient client, String token, NetworkPoolServer poolServer, Map opts = [:]) {
		opts.doPaging = true
		def listResults = listNetworks(client, token, poolServer, opts)

		if(listResults.success && !listResults.error && listResults.data) {
			List apiItems = listResults.data as List<Map>
			Observable<NetworkPoolIdentityProjection> poolRecords = morpheus.network.pool.listIdentityProjections(poolServer.id)
			SyncTask<NetworkPoolIdentityProjection,Map,NetworkPool> syncTask = new SyncTask(poolRecords, apiItems as Collection<Map>)
			syncTask.addMatchFunction { NetworkPoolIdentityProjection domainObject, Map apiItem ->
				domainObject.externalId == "${apiItem.id}"
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
		def poolType = new NetworkPoolType(code: 'morpheusuplink')
		def poolTypeIpv6 = new NetworkPoolType(code: 'morpheusuplinkipv6')
		List<NetworkPool> missingPoolsList = []
		chunkedAddList?.each { Map it ->
            if (it.poolEnabled) {
                def id = it.id
                def newNetworkPool
                def name = it.name
                def rangeConfig
                def addRange
                def cidr
                def startAddress
                def endAddress
                def addressCount

                if(!it.type.code.contains('ipv6')) {
                    def addConfig = [account:poolServer.account, poolServer:poolServer, owner:poolServer.account, name:name, externalId:"${id}",
                                    type: poolType, poolEnabled:true, parentType:'NetworkPoolServer', parentId:poolServer.id,ipCount:it.ipCount]
                    newNetworkPool = new NetworkPool(addConfig)
                    newNetworkPool.ipRanges = []
                    it.ipRanges?.each { range ->
                        cidr = range?.cidr
                        startAddress = range?.startAddress ?: null
                        endAddress = range?.endAddress ?: null
                        addressCount = range?.addressCount ?: null
                        rangeConfig = [externalId:range.id,cidr:cidr, startAddress:startAddress.toString(), endAddress:endAddress.toString(), addressCount:addressCount]
                        addRange = new NetworkPoolRange(rangeConfig)
                        newNetworkPool.ipRanges.add(addRange)
                    }
                } else {
                    def addConfig = [account:poolServer.account, poolServer:poolServer, owner:poolServer.account, name:name, externalId:"${id}",
                                    type: poolTypeIpv6, poolEnabled:true, parentType:'NetworkPoolServer', parentId:poolServer.id,ipCount:it.ipCount]
                    newNetworkPool = new NetworkPool(addConfig)
                    newNetworkPool.ipRanges = []
                    it.ipRanges?.each { range ->
                        cidr = range?.cidrIPv6 ?: range?.cidr ?: null
                        startAddress = range?.startAddress ?: null
                        endAddress = range?.endAddress ?: null
                        addressCount = range?.addressCount ?: null
                        rangeConfig = [externalId:range.id,cidrIPv6:cidr, startIPv6Address:startAddress,endIPv6Address:endAddress, addressCount:addressCount]
                        addRange = new NetworkPoolRange(rangeConfig)
                        newNetworkPool.ipRanges.add(addRange)
                    }
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
				
                if(existingItem?.displayName != network.displayName) {
					existingItem.displayName = network.displayName
					save = true
				}
                if(existingItem?.name != network.name) {
					existingItem.name = network.name
					save = true
				}
                if(existingItem?.ipCount != network.ipCount) {
					existingItem.ipCount = network.ipCount
					save = true
				}
				if(existingItem?.cidr != network.cidr) {
					existingItem.cidr = network.cidr
					save = true
				}
                // if(existingItem?.startAddress != network.cidrIPv6) {
				// 	existingItem.startAddress = network.cidrIPv6
				// 	save = true
				// }
                // if(existingItem?.startAddress != network.startAddress) {
				// 	existingItem.startAddress = network.startAddress
				// 	save = true
				// }
                // if(existingItem?.endAddress != network.endAddress) {
				// 	existingItem.endAddress = network.endAddress
				// 	save = true
				// }
                // if(existingItem?.addressCount != network.addressCount) {
				// 	existingItem.addressCount = network.addressCount
				// 	save = true
				// }
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
	def cacheZones(HttpApiClient client,String token,NetworkPoolServer poolServer, Map opts = [:]) {
		try {
			def listResults = listZones(client,token,poolServer, opts)

			log.info("listZoneResults: {}", listResults)
			if (listResults.success && listResults.data != null) {
				List apiItems = listResults.data as List<Map>
				Observable<NetworkDomainIdentityProjection> domainRecords = morpheus.network.domain.listIdentityProjections(poolServer.integration.id)

				SyncTask<NetworkDomainIdentityProjection,Map,NetworkDomain> syncTask = new SyncTask(domainRecords, apiItems as Collection<Map>)
				syncTask.addMatchFunction { NetworkDomainIdentityProjection domainObject, Map apiItem ->
					domainObject.externalId == "${apiItem.id}"
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
	
    private ServiceResponse listZones(HttpApiClient client, String token, NetworkPoolServer poolServer, Map opts = [:]) {
        def rtn = new ServiceResponse()
        rtn.data = [] // Initialize rtn.data as an empty list
        try {
            def rpcConfig = getRpcConfig(poolServer)
            def apiUrl = cleanServiceUrl(rpcConfig.serviceUrl)
            def apiPath = getServicePath(rpcConfig.serviceUrl) + networkDomainsPath
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
                    requestOptions.headers = [Authorization: "Bearer ${token}".toString()]
                    requestOptions.queryParams = [max:maxResults.toString(),offset:start.toString()]

                    def results = client.callJsonApi(apiUrl,apiPath,null,null,requestOptions,'GET')

                    if(results?.success && results?.error != true) {
                        rtn.success = true
                        if(results.data?.networkDomains?.size() > 0) {
                            rtn.data += results.data.networkDomains

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
                requestOptions.headers = [Authorization: "Bearer ${token}".toString()]
                requestOptions.queryParams = [max:maxResults.toString(),offset:start.toString()]

                def results = client.callJsonApi(apiUrl,apiPath,null,null,requestOptions,'GET')

                if(results?.success && results?.error != true) {
                    rtn.success = true
                    if(results.data?.networkDomains?.size() > 0) {
                        rtn.data = results.data.networkDomains
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
		List<NetworkDomain> missingZonesList = addList?.findAll { Map add ->
            def domainName = NetworkUtility.getFriendlyDomainName(add.name as String)
            domainName != 'localdomain' && add.active == true
        }?.collect { Map add ->
            NetworkDomain networkDomain = new NetworkDomain()
            networkDomain.externalId = add.id
            networkDomain.name = NetworkUtility.getFriendlyDomainName(add.name as String)
            networkDomain.fqdn = NetworkUtility.getFqdnDomainName(add.fqdn as String)
            networkDomain.displayName = NetworkUtility.getFriendlyDomainName(add?.displayName ?: null as String)
            networkDomain.refSource = 'integration'
            networkDomain.zoneType = 'Authoritative'
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
					existingItem.externalId = update.masterItem.id
					save = true
				}
                if(existingItem.name != update.masterItem.name) {
                    existingItem.name = update.masterItem.name
                    save = true
                }
                if(existingItem.displayName != update.masterItem?.displayName) {
                    existingItem.displayName = update.masterItem?.displayName
                    save = true
                }
                if(existingItem.fqdn != update.masterItem.fqdn) {
                    existingItem.fqdn = update.masterItem.fqdn
                    save = true
                }
				if(!existingItem.refId) {
					existingItem.refType = 'Authoritative'
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

    def cacheZoneRecords(HttpApiClient client,String token, NetworkPoolServer poolServer, Map opts) {
		morpheus.network.domain.listIdentityProjections(poolServer.integration.id).flatMap {NetworkDomainIdentityProjection domain ->
			Completable.mergeArray(cacheZoneDomainRecords(client,token,poolServer,domain,'A',opts),
				cacheZoneDomainRecords(client,token,poolServer, domain, 'AAAA', opts),
				cacheZoneDomainRecords(client,token,poolServer, domain, 'PTR', opts),
				cacheZoneDomainRecords(client,token,poolServer, domain, 'TXT', opts),
				cacheZoneDomainRecords(client,token,poolServer, domain, 'CNAME', opts),
				cacheZoneDomainRecords(client,token,poolServer, domain, 'MX', opts)
			).toObservable().subscribeOn(Schedulers.io())
		}.doOnError{ e ->
			log.error("cacheZoneRecords error: ${e}", e)
		}.subscribe()
	}  

	Completable cacheZoneDomainRecords(HttpApiClient client,String token, NetworkPoolServer poolServer, NetworkDomainIdentityProjection domain, String recordType, Map opts) {
		log.debug "cacheZoneDomainRecords $poolServer, $domain, $recordType, $opts"
		def listResults = listZoneRecords(client,token, poolServer, domain, recordType, opts)
		log.debug("listResults: {}",listResults)
		if(listResults.success) {
			List<Map> apiItems = listResults.data as List<Map>
			Observable<NetworkDomainRecordIdentityProjection> domainRecords = morpheus.network.domain.record.listIdentityProjections(domain,recordType)
			SyncTask<NetworkDomainRecordIdentityProjection,Map,NetworkDomainRecord> syncTask = new SyncTask(domainRecords, apiItems as Collection<Map>)
			syncTask.addMatchFunction { NetworkDomainRecordIdentityProjection domainObject, Map apiItem ->
				domainObject.externalId == apiItem.id
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
                def addConfig = [networkDomain: new NetworkDomain(id: domain.id), externalId:it.id,ttl: it.ttl?.toInteger(), name: it.name, content: it.content, fqdn: "${it.name}.${domain.name ?: ""}", type: recordType, source: 'sync']

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
                def fqdn = update.masterItem.fqdn
                def data = update.masterItem.content
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

    ServiceResponse listZoneRecords(HttpApiClient client,String token, NetworkPoolServer poolServer, NetworkDomainIdentityProjection domain, String recordType = 'A', Map opts=[:]) {
        def rtn = new ServiceResponse()
        rtn.data = [] // Initialize rtn.data as an empty list
        try {
            def rpcConfig = getRpcConfig(poolServer)
            def apiUrl = cleanServiceUrl(rpcConfig.serviceUrl)
            def apiPath = getServicePath(rpcConfig.serviceUrl) + networkDomainsPath + domain.externalId + '/records'
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
                    requestOptions.headers = [Authorization: "Bearer ${token}".toString()]
                    requestOptions.queryParams = [max:maxResults.toString(),offset:start.toString()]

                    def results = client.callJsonApi(apiUrl,apiPath,null,null,requestOptions,'GET')

                    if(results?.success && results?.error != true) {
                        rtn.success = true
                        if(results.data?.networkDomainRecords?.size() > 0) {
                            results.data.networkDomainRecords.each { item ->
                                if (item.type == recordType) {
                                    rtn.data.add(item)
                                }
                            }

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
                requestOptions.headers = [Authorization: "Bearer ${token}".toString()]
                requestOptions.queryParams = [max:maxResults.toString(),offset:start.toString()]

                def results = client.callJsonApi(apiUrl,apiPath,null,null,requestOptions,'GET')

                if(results?.success && results?.error != true) {
                    rtn.success = true
                    if(results.data?.networkDomainRecords?.size() > 0) {
                        results.data.networkDomainRecords.each { item ->
                            if (item.type == recordType) {
                                rtn.data.add(item)
                            }
                        }
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
        def token
        def rpcConfig = getRpcConfig(poolServer)

        try {
			if(integration) {
                token = login(client,rpcConfig)
                if(token.success) {
                    token = token?.token as String
                    def fqdn = record.name
                    if(record.name.endsWith(record.networkDomain.name)) {
                        fqdn = record.name.tokenize('.')[0]
                    }

                    HttpApiClient.RequestOptions requestOptions = new HttpApiClient.RequestOptions(ignoreSSL: rpcConfig.ignoreSSL)
                    requestOptions.headers = [Authorization: "Bearer ${token}".toString()]
                    def apiUrl = cleanServiceUrl(rpcConfig.serviceUrl)
                    def recordType = record.type
                    def apiPath = getServicePath(rpcConfig.serviceUrl) + networkDomainsPath + domain.externalId.toString() + '/records'
                    def results = new ServiceResponse()
                    requestOptions.body = ['networkDomainRecord':['name':hostname,'fqdn':hostname,'content':networkPoolIp.ipAddress,'ttl':3600,'type':recordType]]

                    results = client.callJsonApi(apiUrl,apiPath,null,null,requestOptions,'POST')

                    if (results.success && !results.error) {
                        domainRecord = new NetworkDomainRecord(networkDomain: domain, networkPoolIp: networkPoolIp, name: hostname, fqdn: hostname, source: 'user', type: recordType, externalId: results.id)
                        morpheus.network.domain.record.create(domainRecord).blockingGet()
                    }
                }
			} else {
                log.warn("API Call Failed to allocate DNS Record")
                return ServiceResponse.error("API Call Failed to allocate DNS Record",null,networkPoolIp)
            }
		} catch(e) {
			log.error("provisionServer error: ${e}", e)
		} finally {
			client.shutdownClient()
		}
		return rtn
	}

    ServiceResponse deleteRecord(AccountIntegration integration, NetworkDomainRecord record, Map opts) {
        HttpApiClient client = new HttpApiClient()
        def rpcConfig = getRpcConfig(poolServer)
        HttpApiClient.RequestOptions requestOptions = new HttpApiClient.RequestOptions(ignoreSSL: rpcConfig.ignoreSSL)
        def rtn = new ServiceResponse()
        def token

        try {
            if(integration) {
                token = login(client,rpcConfig)
                if(token.success) {
                    token = token?.token as String
                    requestOptions.headers = [Authorization: "Bearer ${token}".toString()]
                    morpheus.network.getPoolServerByAccountIntegration(integration).doOnSuccess({ poolServer ->
                        def apiUrl = cleanServiceUrl(rpcConfig.serviceUrl)
                        try {
                            def apiPath = getServicePath(rpcConfig.serviceUrl) + networkDomainsPath + domain.externalId.toString() + '/records/' + record.externalId.toString()

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
                }
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
        def domainRecord
        def token

        try {
            token = login(client,rpcConfig)
            if(token.success) {
                token = token?.token as String
                HttpApiClient.RequestOptions requestOptions = new HttpApiClient.RequestOptions(ignoreSSL: rpcConfig.ignoreSSL)
                requestOptions.headers = [Authorization: "Bearer ${token}".toString()]
                def hostname = networkPoolIp.hostname
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

                    requestOptions.body = ['networkPoolIp':['hostname':hostname,'ipAddress':networkPoolIp.ipAddress]]
                } else {
                    // Grab next available IP
                    requestOptions.body = ['networkPoolIp':['hostname':hostname]]
                }
                
                apiPath = getServicePath(rpcConfig.serviceUrl) + networkPoolsPath + networkPool.externalId.toString() + '/ips'
                results = client.callJsonApi(apiUrl,apiPath,null,null,requestOptions,'POST')

                if(results.success && !results.error) {
                    networkPoolIp.externalId = results.data.networkPoolIp.id
                    networkPoolIp.ipAddress = results.data.networkPoolIp.ipAddress
                    return ServiceResponse.success(networkPoolIp)
                } else {
                    log.warn("API Call Failed to allocate IP Address")
                    return ServiceResponse.error("API Call Failed to allocate IP Address",null,networkPoolIp)
                }

                if (networkPoolIp.id) {
                    networkPoolIp = morpheus.network.pool.poolIp.save(networkPoolIp)?.blockingGet()
                } else {
                    networkPoolIp = morpheus.network.pool.poolIp.create(networkPoolIp)?.blockingGet()
                }

                if (networkPool.type.code == 'morpheusuplink'){
                    recordType = 'A'
                } else {
                    recordType = 'AAAA'
                }
                if(createARecord) {
                    networkPoolIp.domain = domain
                }

                if (createARecord && domain && domain.refId == poolServer.integration.id) {
                    log.info("Attempting DNS record...")

                    apiPath = getServicePath(rpcConfig.serviceUrl) + networkDomainsPath + domain.externalId.toString() + '/records'
                    requestOptions.body = ['networkDomainRecord':['name':hostname,'fqdn':hostname,'content':networkPoolIp.ipAddress,'ttl':3600,'type':recordType]]

                    results = client.callJsonApi(apiUrl,apiPath,null,null,requestOptions,'POST')

                    if (results.success && !results.error) {
                        domainRecord = new NetworkDomainRecord(networkDomain: domain, networkPoolIp: networkPoolIp, name: hostname, fqdn: hostname, source: 'user', type: recordType, externalId: results.data.id)
                        morpheus.network.domain.record.create(domainRecord).blockingGet()
                    }
                }

                if (results.success && !results.error) {
                    return ServiceResponse.success(domainRecord)
                } else {
                    log.warn("API Call Failed to allocate DNS Record")
                    return ServiceResponse.error("API Call Failed to allocate DNS Record",null,networkPoolIp)
                }
            } else {
                return ServiceResponse.error("Error Authenticating to Morpheus Uplink IPAM - ${poolServer.name} during host record creation.")
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
        def token

        try {
            token = login(client,rpcConfig)
            if(token.success) {
                token = token?.token as String
                def results = []
                def hostname = networkPoolIp.hostname
                def customProperty = poolServer.configMap?.nameProperty
                requestOptions.headers = [Authorization: "Bearer ${token}".toString()]
                def apiUrl = cleanServiceUrl(rpcConfig.serviceUrl)
                def apiPath = getServicePath(rpcConfig.serviceUrl) + networkPoolsPath + networkPool.externalId.toString() + '/ips/' + networkPoolIp.externalId.toString()
                requestOptions.queryParams = [:]
                requestOptions.body = ['networkPoolIp':['hostname':hostname]]

                results = client.callJsonApi(apiUrl,apiPath,null,null,requestOptions,'PUT')

                if (results.success && !results.error) {
                        return ServiceResponse.success(networkPoolIp)
                    } else {
                        return ServiceResponse.error(results.error ?: 'Error Updating Host Record', null, networkPoolIp)
                    }
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
        def token

        try {
            token = login(client,rpcConfig)
            if(token.success) {
                token = token?.token as String
                def results = []
                requestOptions.headers = [Authorization: "Bearer ${token}".toString()]
                def apiUrl = cleanServiceUrl(rpcConfig.serviceUrl)
                def apiPath = getServicePath(rpcConfig.serviceUrl) + networkPoolsPath + networkPool.externalId.toString() + '/ips/' + networkPoolIp.externalId.toString()

                results = client.callJsonApi(apiUrl,apiPath,null,null,requestOptions,'DELETE')

                if(results.success && !results.error) {
                    return ServiceResponse.success(networkPoolIp)
                } else {
                    log.error("Error Deleting Host Record ${networkPoolIp}")
                    return ServiceResponse.error("Error Deleting Host Record ${networkPoolIp}")
                }
            }
        } catch(x) {
            log.error("Error Deleting Host Record {}",x.message,x)
            return ServiceResponse.error("Error Deleting Host Record ${x.message}",null,networkPoolIp)
        } finally {
            client.shutdownClient()
        }
	}

	private ServiceResponse listNetworks(HttpApiClient client, String token, NetworkPoolServer poolServer, Map opts = [:]) {
        def rtn = new ServiceResponse()
        rtn.data = [] // Initialize rtn.data as an empty list
        try {
            def rpcConfig = getRpcConfig(poolServer)
            def apiUrl = cleanServiceUrl(rpcConfig.serviceUrl)
            def apiPath = getServicePath(rpcConfig.serviceUrl) + networkPoolsPath
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
                    requestOptions.headers = [Authorization: "Bearer ${token}".toString()]
                    requestOptions.queryParams = [max:maxResults.toString(),offset:start.toString()]

                    def results = client.callJsonApi(apiUrl,apiPath,null,null,requestOptions,'GET')

                    if(results?.success && results?.error != true) {
                        rtn.success = true
                        if(results?.data?.networkPools?.size() > 0) {
                            rtn.data += results.data.networkPools

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
                requestOptions.headers = [Authorization: "Bearer ${token}".toString()]
                requestOptions.queryParams = [max:maxResults.toString(),offset:start.toString()]

                def results = client.callJsonApi(apiUrl,apiPath,null,null,requestOptions,'GET')

                if(results?.success && results?.error != true) {
                    rtn.success = true
                    if(results?.data?.networkPools?.size() > 0) {
                        rtn.data = results.data.networkPools
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
    void cacheIpAddressRecords(HttpApiClient client,String token, NetworkPoolServer poolServer, Map opts=[:]) {
        morpheus.network.pool.listIdentityProjections(poolServer.id).buffer(50).flatMap { Collection<NetworkPoolIdentityProjection> poolIdents ->
            return morpheus.network.pool.listById(poolIdents.collect{it.id})
        }.flatMap { NetworkPool pool ->
            def listResults = listHostRecords(client,token,poolServer,pool)
            if (listResults.success && !listResults.error && listResults.data) {
                List<Map> apiItems = listResults.data
                Observable<NetworkPoolIpIdentityProjection> poolIps = morpheus.network.pool.poolIp.listIdentityProjections(pool.id)
                SyncTask<NetworkPoolIpIdentityProjection, Map, NetworkPoolIp> syncTask = new SyncTask<NetworkPoolIpIdentityProjection, Map, NetworkPoolIp>(poolIps, apiItems)
                return syncTask.addMatchFunction { NetworkPoolIpIdentityProjection domainObject, Map apiItem ->
                    domainObject.externalId == "${apiItem.id}"
                }.addMatchFunction { NetworkPoolIpIdentityProjection domainObject, Map apiItem ->
                    domainObject.ipAddress == apiItem.ipAddress
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
                return Single.just(false).toObservable()
            }
        }.doOnError{ e ->
            log.error("cacheIpRecords error: ${e}", e)
        }.subscribe()

    }

	void addMissingIps(NetworkPool pool, List addList) {
        List<NetworkPoolIp> poolIpsToAdd = addList?.collect { it ->
            def externalId = it.id
			def ipAddress = it.ipAddress
			def ipType = it.ipType ?: 'assigned'
            def hostname = it.hostname
			def addConfig = [networkPool: pool, networkPoolRange: pool.ipRanges ? pool.ipRanges.first() : null, ipType: ipType, hostname: hostname, ipAddress: ipAddress.toString(), externalId:externalId.toString()]
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
				def hostname = update.masterItem.hostname
				def ipType = update.masterItem.ipType
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
			morpheus.network.pool.poolIp.save(ipsToUpdate).blockingGet()
		}
	}


	private ServiceResponse listHostRecords(HttpApiClient client,String token, NetworkPoolServer poolServer,NetworkPool networkPool, Map opts = [:]) {
        def rtn = new ServiceResponse()
        rtn.data = [] // Initialize rtn.data as an empty list
        try {
            def rpcConfig = getRpcConfig(poolServer)
            def apiUrl = cleanServiceUrl(rpcConfig.serviceUrl)
            def apiPath = getServicePath(rpcConfig.serviceUrl) + networkPoolsPath + networkPool.externalId.toString() + '/ips'
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
                    requestOptions.headers = [Authorization: "Bearer ${token}".toString()]
                    requestOptions.queryParams = [max:maxResults.toString(),offset:start.toString()]

                    def results = client.callJsonApi(apiUrl,apiPath,null,null,requestOptions,'GET')

                    if(results?.success && results?.error != true) {
                        rtn.success = true
                        if(results.data?.networkPoolIps?.size() > 0) {
                            rtn.data += results.data?.networkPoolIps

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
                requestOptions.headers = [Authorization: "Bearer ${token}".toString()]
                requestOptions.queryParams = [max:maxResults.toString(),offset:start.toString()]

                def results = client.callJsonApi(apiUrl,apiPath,null,null,requestOptions,'GET')

                if(results?.success && results?.error != true) {
                    rtn.success = true
                    if(results.data?.networkPoolIps?.size() > 0) {
                        rtn.data = results.data?.networkPoolIps
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


	ServiceResponse testNetworkPoolServer(HttpApiClient client, String token, NetworkPoolServer poolServer) {
		def rtn = new ServiceResponse()
		try {
			def opts = [doPaging:false, maxResults:1]
			def networkList = listNetworks(client,token,poolServer, opts)
			rtn.success = networkList.success
			rtn.data = [:]
			if(!networkList.success) {
				rtn.msg = 'Error connecting to Morpheus'
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
			new NetworkPoolType(code:'morpheusuplink', name:'Morpheus Uplink', creatable:false, description:'Morpheus Uplink', rangeSupportsCidr: false),
			new NetworkPoolType(code:'morpheusuplinkipv6', name:'Morpheus Uplink IPv6', creatable:false, description:'Morpheus Uplink IPv6', rangeSupportsCidr: true, ipv6Pool:true)
		]
	}

	/**
	 * Provide custom configuration options when creating a new {@link AccountIntegration}
	 * @return a List of OptionType
	 */
	@Override
	List<OptionType> getIntegrationOptionTypes() {
		return [
				new OptionType(code: 'morpheus.serviceUrl', name: 'Service URL', inputType: OptionType.InputType.TEXT, fieldName: 'serviceUrl', fieldLabel: 'API Url', fieldContext: 'domain', placeHolder: 'https://x.x.x.x/', displayOrder: 0, required:true),
				new OptionType(code: 'morpheus.credentials', name: 'Credentials', inputType: OptionType.InputType.CREDENTIAL, fieldName: 'type', fieldLabel: 'Credentials', fieldContext: 'credential', required: true, displayOrder: 1, defaultValue: 'local',optionSource: 'credentials',config: '{"credentialTypes":["username-password"]}'),

				new OptionType(code: 'morpheus.serviceUsername', name: 'Service Username', inputType: OptionType.InputType.TEXT, fieldName: 'serviceUsername', fieldLabel: 'Username', fieldContext: 'domain', displayOrder: 2,localCredential: true, required: true),
				new OptionType(code: 'morpheus.servicePassword', name: 'Service Password', inputType: OptionType.InputType.PASSWORD, fieldName: 'servicePassword', fieldLabel: 'Password', fieldContext: 'domain', displayOrder: 3,localCredential: true, required: true),
				new OptionType(code: 'morpheus.throttleRate', name: 'Throttle Rate', inputType: OptionType.InputType.NUMBER, defaultValue: 0, fieldName: 'serviceThrottleRate', fieldLabel: 'Throttle Rate', fieldContext: 'domain', displayOrder: 4),
				new OptionType(code: 'morpheus.ignoreSsl', name: 'Ignore SSL', inputType: OptionType.InputType.CHECKBOX, defaultValue: false, fieldName: 'ignoreSsl', fieldLabel: 'Disable SSL SNI Verification', fieldContext: 'domain', displayOrder: 5),
				new OptionType(code: 'morpheus.inventoryExisting', name: 'Inventory Existing', inputType: OptionType.InputType.CHECKBOX, defaultValue: false, fieldName: 'inventoryExisting', fieldLabel: 'Inventory Existing', fieldContext: 'config', displayOrder: 6)
		]
	}

	@Override
	Icon getIcon() {
		return new Icon(path:"morpheusuplink.svg", darkPath: "morpheusuplink-dark.svg")
	}

    def login(HttpApiClient client, rpcConfig) {
        def rtn = [success:false]
        try {
            HttpApiClient.RequestOptions requestOptions = new HttpApiClient.RequestOptions(ignoreSSL: rpcConfig.ignoreSSL,contentType: 'form')

            def username = rpcConfig.username.toString()
            def password = rpcConfig.password.toString()

            requestOptions.queryParams = ['client_id':'morph-automation','grant_type':'password','scope':'write']
            requestOptions.headers = ['Content-Type':'application/x-www-form-urlencoded']

            // requestOptions.body = "username=${username}&password=${password}"
            requestOptions.body = ['username':username,'password':password]

            def apiUrl = cleanServiceUrl(rpcConfig.serviceUrl)
            def apiPath = getServicePath(rpcConfig.serviceUrl) + authPath

            def results = client.callJsonApi(apiUrl,apiPath,requestOptions,'POST')
            if(results?.success && results?.error != true) {
                log.debug("login: ${results}")
                rtn.token = results.data?.access_token?.trim()
                rtn.success = true
            } else {
                return ServiceResponse.error("Get Token Error")
            }
        } catch(e) {
            log.error("Get Token Error: ${e}", e)
            return ServiceResponse.error("Failed to authenticate to Morpheus")
        }
        return rtn
    }

    void logout(HttpApiClient client, rpcConfig, String token) {
        try {
            def apiUrl = cleanServiceUrl(rpcConfig.serviceUrl)
            def apiPath = getServicePath(rpcConfig.serviceUrl) + 'logout'
            HttpApiClient.RequestOptions requestOptions = new HttpApiClient.RequestOptions(ignoreSSL: rpcConfig.ignoreSSL)
            requestOptions.headers = [Authorization: "Bearer ${token}".toString()]
            client.callJsonApi(apiUrl,apiPath,requestOptions,"GET")
        } catch(e) {
            log.error("logout error: ${e}", e)
        }
    }

    private getRpcConfig(NetworkPoolServer poolServer) {
        return [
            username:poolServer.credentialData?.username ?: poolServer.serviceUsername,
            password:poolServer.credentialData?.password ?: poolServer.servicePassword,
            serviceUrl:poolServer.serviceUrl,
            ignoreSSL:poolServer.ignoreSsl
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
