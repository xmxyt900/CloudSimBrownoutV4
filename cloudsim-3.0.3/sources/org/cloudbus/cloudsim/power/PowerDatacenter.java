/*
 * Title:        CloudSim Toolkit
 * Description:  CloudSim (Cloud Simulation) Toolkit for Modeling and Simulation of Clouds
 * Licence:      GPL - http://www.gnu.org/copyleft/gpl.html
 *
 * Copyright (c) 2009-2012, The University of Melbourne, Australia
 */

package org.cloudbus.cloudsim.power;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.CloudletOptionalComponent;
import org.cloudbus.cloudsim.Datacenter;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.ResCloudlet;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.VmAllocationPolicy;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEvent;
import org.cloudbus.cloudsim.core.predicates.PredicateType;

import com.cloudbus.cloudsim.examples.power.brownout.BrownoutConstants;
import com.cloudbus.cloudsim.examples.power.brownout.DimmerConstants;

/**
 * PowerDatacenter is a class that enables simulation of power-aware data
 * centers.
 * 
 * If you are using any algorithms, policies or workload included in the power
 * package please cite the following paper:
 * 
 * Anton Beloglazov, and Rajkumar Buyya, "Optimal Online Deterministic
 * Algorithms and Adaptive Heuristics for Energy and Performance Efficient
 * Dynamic Consolidation of Virtual Machines in Cloud Data Centers", Concurrency
 * and Computation: Practice and Experience (CCPE), Volume 24, Issue 13, Pages:
 * 1397-1420, John Wiley & Sons, Ltd, New York, USA, 2012
 * 
 * @author Anton Beloglazov
 * @since CloudSim Toolkit 2.0
 */
public class PowerDatacenter extends Datacenter {

	/** The power. */
	private double power;

	/** The disable migrations. */
	private boolean disableMigrations;

	/** The cloudlet submited. */
	private double cloudletSubmitted;

	/** The migration count. */
	private int migrationCount;
	
	/** The number of times that the dimmer is triggered*/
	private int dimmerTimes = 0;
	
	/** A set that records how many times dimmer maybe triggered*/
	private Set<Double> timeFrameMayTriggeredDimmer = new HashSet<Double>();
	
	/** A lined hashmap records how many active hosts at each time interval*/
	private LinkedHashMap<Double, Integer> numberOfActiveHostMap = new LinkedHashMap<Double, Integer>();
	
	/**
	 * Instantiates a new datacenter.
	 * 
	 * @param name
	 *            the name
	 * @param characteristics
	 *            the res config
	 * @param schedulingInterval
	 *            the scheduling interval
	 * @param utilizationBound
	 *            the utilization bound
	 * @param vmAllocationPolicy
	 *            the vm provisioner
	 * @param storageList
	 *            the storage list
	 * @throws Exception
	 *             the exception
	 */
	public PowerDatacenter(String name, DatacenterCharacteristics characteristics,
			VmAllocationPolicy vmAllocationPolicy, List<Storage> storageList, double schedulingInterval)
					throws Exception {
		super(name, characteristics, vmAllocationPolicy, storageList, schedulingInterval);

		setPower(0.0);
		setDisableMigrations(false);
		setCloudletSubmitted(-1);
		setMigrationCount(0);
	}

	/**
	 * Updates processing of each cloudlet running in this PowerDatacenter. It
	 * is necessary because Hosts and VirtualMachines are simple objects, not
	 * entities. So, they don't receive events and updating cloudlets inside
	 * them must be called from the outside.
	 * 
	 * @pre $none
	 * @post $none
	 */
	@Override
	protected void updateCloudletProcessing() {
		if (getCloudletSubmitted() == -1 || getCloudletSubmitted() == CloudSim.clock()) {
			CloudSim.cancelAll(getId(), new PredicateType(CloudSimTags.VM_DATACENTER_EVENT));
			schedule(getId(), getSchedulingInterval(), CloudSimTags.VM_DATACENTER_EVENT);
			return;
		}
		double currentTime = CloudSim.clock();

		// if some time passed  since last processing
		if (currentTime > getLastProcessTime()) {
			System.out.print(currentTime + " ");

			double minTime = updateCloudetProcessingWithoutSchedulingFutureEventsForce();

			if (!isDisableMigrations()) {
				List<Map<String, Object>> migrationMap = getVmAllocationPolicy().optimizeAllocation(getVmList());

				if (migrationMap != null) {
					for (Map<String, Object> migrate : migrationMap) {
						Vm vm = (Vm) migrate.get("vm");
						PowerHost targetHost = (PowerHost) migrate.get("host");
						PowerHost oldHost = (PowerHost) vm.getHost();

						if (oldHost == null) {
							Log.formatLine("%.2f: Migration of VM #%d to Host #%d is started", currentTime, vm.getId(),
									targetHost.getId());
						} else {
							Log.formatLine("%.2f: Migration of VM #%d from Host #%d to Host #%d is started",
									currentTime, vm.getId(), oldHost.getId(), targetHost.getId());
						}

						targetHost.addMigratingInVm(vm);
						incrementMigrationCount();

						/** VM migration delay = RAM / bandwidth **/
						// we use BW / 2 to model BW available for migration
						// purposes, the other
						// half of BW is for VM communication
						// around 16 seconds for 1024 MB using 1 Gbit/s network
						send(getId(), vm.getRam() / ((double) targetHost.getBw() / (2 * 8000)), CloudSimTags.VM_MIGRATE,
								migrate);
					}
				}
			}

			// schedules an event to the next time
			if (minTime != Double.MAX_VALUE) {
				CloudSim.cancelAll(getId(), new PredicateType(CloudSimTags.VM_DATACENTER_EVENT));
				send(getId(), getSchedulingInterval(), CloudSimTags.VM_DATACENTER_EVENT);
			}

			setLastProcessTime(currentTime);
		}
	}

	/**
	 * Update cloudlet processing without scheduling future events.
	 * 
	 * @return the double
	 */
	protected double updateCloudetProcessingWithoutSchedulingFutureEvents() {
		if (CloudSim.clock() > getLastProcessTime()) {
			return updateCloudetProcessingWithoutSchedulingFutureEventsForce();
		}
		return 0;
	}

	/**
	 * Update cloudet processing without scheduling future events.
	 * 
	 * @return the double
	 */
	protected double updateCloudetProcessingWithoutSchedulingFutureEventsForce() {
		double currentTime = CloudSim.clock();
		double minTime = Double.MAX_VALUE;
		double timeDiff = currentTime - getLastProcessTime();
		double timeFrameDatacenterEnergy = 0.0;
		int numberOfActiveHost = 0;

		Log.printLine("\n\n--------------------------------------------------------------\n\n");
		Log.formatLine("New resource usage for the time frame starting at %.2f:", currentTime);
		double dimmerValue = getDimmerValue();

		for (PowerHost host : this.<PowerHost> getHostList()) {
			Log.printLine();

			// Dimmer is triggered
			triggerDimmer(host, currentTime, dimmerValue);

			double time = host.updateVmsProcessing(currentTime); // inform VMs
																	// to update
																	// processing
			if (time < minTime) {
				minTime = time;
			}

			Log.formatLine("%.2f: [Host #%d] utilization is %.2f%%", currentTime, host.getId(),
					host.getUtilizationOfCpu() * 100);
			

		}
		

		if (timeDiff > 0) {
			Log.formatLine("\nEnergy consumption for the last time frame from %.2f to %.2f:", getLastProcessTime(),
					currentTime);

			//Added the code that recored time frame that may trigger dimmer
			timeFrameMayTriggeredDimmer.add(currentTime); 
			
			for (PowerHost host : this.<PowerHost> getHostList()) {
				double previousUtilizationOfCpu = host.getPreviousUtilizationOfCpu();
				double utilizationOfCpu = host.getUtilizationOfCpu();
				double timeFrameHostEnergy = host.getEnergyLinearInterpolation(previousUtilizationOfCpu,
						utilizationOfCpu, timeDiff);
				timeFrameDatacenterEnergy += timeFrameHostEnergy;

				Log.printLine();
				Log.formatLine("%.2f: [Host #%d] utilization at %.2f was %.2f%%, now is %.2f%%", currentTime,
						host.getId(), getLastProcessTime(), previousUtilizationOfCpu * 100, utilizationOfCpu * 100);
				Log.formatLine("%.2f: [Host #%d] energy is %.2f W*sec", currentTime, host.getId(), timeFrameHostEnergy);
				
				if(host.getUtilizationOfCpu() == 0){ //Compute the total active number of host at each time interval
					numberOfActiveHost++;
				}
				
			}

			
			Log.formatLine("\n%.2f: Data center's energy is %.2f W*sec\n", currentTime, timeFrameDatacenterEnergy);
		}
		
		if(0 == (currentTime - 0.1) % 300){
		numberOfActiveHostMap.put(currentTime - 0.1, numberOfActiveHost); //Record the active number of host into a map
		}

		setPower(getPower() + timeFrameDatacenterEnergy);

		checkCloudletCompletion();

		/** Remove completed VMs **/
		for (PowerHost host : this.<PowerHost> getHostList()) {
			for (Vm vm : host.getCompletedVms()) {
				getVmAllocationPolicy().deallocateHostForVm(vm);
				getVmList().remove(vm);
				Log.printLine("VM #" + vm.getId() + " has been deallocated from host #" + host.getId());
			}
		}

		Log.printLine();

		setLastProcessTime(currentTime);
		return minTime;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.cloudbus.cloudsim.Datacenter#processVmMigrate(org.cloudbus.cloudsim.
	 * core.SimEvent, boolean)
	 */
	@Override
	protected void processVmMigrate(SimEvent ev, boolean ack) {
		updateCloudetProcessingWithoutSchedulingFutureEvents();
		super.processVmMigrate(ev, ack);
		SimEvent event = CloudSim.findFirstDeferred(getId(), new PredicateType(CloudSimTags.VM_MIGRATE));
		if (event == null || event.eventTime() > CloudSim.clock()) {
			updateCloudetProcessingWithoutSchedulingFutureEventsForce();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see cloudsim.Datacenter#processCloudletSubmit(cloudsim.core.SimEvent,
	 * boolean)
	 */
	@Override
	protected void processCloudletSubmit(SimEvent ev, boolean ack) {
		super.processCloudletSubmit(ev, ack);
		setCloudletSubmitted(CloudSim.clock());
	}

	/**
	 * Gets the power.
	 * 
	 * @return the power
	 */
	public double getPower() {
		return power;
	}

	/**
	 * Sets the power.
	 * 
	 * @param power
	 *            the new power
	 */
	protected void setPower(double power) {
		this.power = power;
	}

	/**
	 * Checks if PowerDatacenter is in migration.
	 * 
	 * @return true, if PowerDatacenter is in migration
	 */
	protected boolean isInMigration() {
		boolean result = false;
		for (Vm vm : getVmList()) {
			if (vm.isInMigration()) {
				result = true;
				break;
			}
		}
		return result;
	}

	/**
	 * Checks if is disable migrations.
	 * 
	 * @return true, if is disable migrations
	 */
	public boolean isDisableMigrations() {
		return disableMigrations;
	}

	/**
	 * Sets the disable migrations.
	 * 
	 * @param disableMigrations
	 *            the new disable migrations
	 */
	public void setDisableMigrations(boolean disableMigrations) {
		this.disableMigrations = disableMigrations;
	}

	/**
	 * Checks if is cloudlet submited.
	 * 
	 * @return true, if is cloudlet submited
	 */
	protected double getCloudletSubmitted() {
		return cloudletSubmitted;
	}

	/**
	 * Sets the cloudlet submited.
	 * 
	 * @param cloudletSubmitted
	 *            the new cloudlet submited
	 */
	protected void setCloudletSubmitted(double cloudletSubmitted) {
		this.cloudletSubmitted = cloudletSubmitted;
	}

	/**
	 * Gets the migration count.
	 * 
	 * @return the migration count
	 */
	public int getMigrationCount() {
		return migrationCount;
	}

	/**
	 * Sets the migration count.
	 * 
	 * @param migrationCount
	 *            the new migration count
	 */
	protected void setMigrationCount(int migrationCount) {
		this.migrationCount = migrationCount;
	}

	/**
	 * Increment migration count.
	 */
	protected void incrementMigrationCount() {
		setMigrationCount(getMigrationCount() + 1);
	}

	/**
	 * Defines when to trigger the dimmer to configure utilization of host
	 * 
	 * @param host
	 * @author minxianx
	 */
	public void triggerDimmer(PowerHost host, double currentTime, double dimmerValue) {
		double utilizationAfterDimmer = 0.0;
		double previousUtilizationOfCpu = host.getPreviousUtilizationOfCpu();
		double vmPreviousUtilizationOfcpu = 0.0;
		int stateSize = 0;
		if (previousUtilizationOfCpu > DimmerConstants.DimmerUpThreshold) {
			dimmerTimes++;
			host.setDimmerValue(dimmerValue);
			host.getDisabaledTagsSet().removeAll(host.getDisabaledTagsSet());

			Log.formatLine("Dimmer is triggered at %.2f:", currentTime);
			Log.formatLine("The dimmer value is %.2f:", dimmerValue);
			for (Vm vm : host.getVmList()) {
				stateSize = vm.getStateHistory().size();
				vmPreviousUtilizationOfcpu = vm.getStateHistory().get(stateSize - 1).getRequestedMips() / vm.getMips();

				for (ResCloudlet rcl : vm.getCloudletScheduler().getCloudletExecList()) {
					updateOptionalComponents(rcl.getCloudlet());
					// rcl.getCloudlet().getUtilizationModelCpu().setUtilization(utilizaiton,
					// CloudSim.clock());
					configureOptionalComponentsByNearestUtilization(host, rcl.getCloudlet(),
							vmPreviousUtilizationOfcpu * (1 - dimmerValue));
					utilizationAfterDimmer = getUtilizaitonAfterDimmer(vmPreviousUtilizationOfcpu, rcl.getCloudlet());
					rcl.getCloudlet().getUtilizationModelCpu().setUtilization(utilizationAfterDimmer, currentTime);

				}
			}
		}
	}
	
	/**
	 * Get the dimmer value. If there are k hosts are overloads, the dimmer value is 1 - k/n, in which n is the total number of host
	 * @return
	 */
	public double getDimmerValue(){
		double dimmerValue = 0.0f;
		double overloadedNumberOfHost = 0;
		for (PowerHost host : this.<PowerHost> getHostList()){
			if(host.getPreviousUtilizationOfCpu() > DimmerConstants.DimmerUpThreshold){
				overloadedNumberOfHost++;
			}
		}
		dimmerValue = 1 - (overloadedNumberOfHost / (double) this.<PowerHost> getHostList().size());
		return dimmerValue;
	}

	/**
	 * Set the specific component of cloudlet as disabled, the
	 * configuredUtilization is the goal utilization that would be reduced. And
	 * this function aims to find the component with utilization nearest to goal
	 * utilization.
	 *
	 * To be optimized......
	 * 
	 * @param cloudlet
	 * @param configuredUtilization
	 * @author minxianx
	 */
	public void configureOptionalComponentsByNearestUtilization(PowerHost host, Cloudlet cloudlet, double configuredUtilization) {
		int nearestUtilizationComponengNumber = 0;
		double minimulUtilizationDifference = 1.0;
		double utilizationDifference = 0.0;
		int index = 0;
		for (CloudletOptionalComponent coc : cloudlet.getCloudletOptionalComponentList()) {
			if (isTagInSet(host.getDisabaledTagsSet(), coc.getComponentTag())) {
				coc.setEnabled(false);
			}

			utilizationDifference = Math.abs(configuredUtilization - coc.getComponentUtilization());
			if (utilizationDifference < minimulUtilizationDifference) {
				minimulUtilizationDifference = utilizationDifference;
				nearestUtilizationComponengNumber = index;
			}
			index++;
		}
		cloudlet.getCloudletOptionalComponentList().get(nearestUtilizationComponengNumber).setEnabled(false);
		host.getDisabaledTagsSet().add(
				cloudlet.getCloudletOptionalComponentList().get(nearestUtilizationComponengNumber).getComponentTag());

		updateHostRevenueLoss(host, cloudlet.getCloudletOptionalComponentList().get(nearestUtilizationComponengNumber));
	}
	
	/**
	 * Set the specific component of cloudlet as disabled, the
	 * configuredUtilization is the goal utilization that would be reduced. And
	 * this function aims to find the component with lowest utilization component
	 * and disable it.
	 *
	 * To be optimized......
	 * 
	 * @param cloudlet
	 * @param configuredUtilization
	 * @author minxianx
	 */
	public void configureOptionalComponentsByLowestUtilization(PowerHost host, Cloudlet cloudlet, double configuredUtilization) {
		int lowestUtilizationComponengNumber = 0;
		double minimulUtilization = 1.0;
		double utilization = 0.0;
		int index = 0;
		for (CloudletOptionalComponent coc : cloudlet.getCloudletOptionalComponentList()) {
			if (isTagInSet(host.getDisabaledTagsSet(), coc.getComponentTag())) {
				coc.setEnabled(false);
			}

			utilization = coc.getComponentUtilization();
			if (utilization < minimulUtilization && utilization > configuredUtilization) {
				minimulUtilization = utilization;
				lowestUtilizationComponengNumber = index;
			}
			index++;
		}
		cloudlet.getCloudletOptionalComponentList().get(lowestUtilizationComponengNumber).setEnabled(false);
		host.getDisabaledTagsSet().add(
				cloudlet.getCloudletOptionalComponentList().get(lowestUtilizationComponengNumber).getComponentTag());

		updateHostRevenueLoss(host, cloudlet.getCloudletOptionalComponentList().get(lowestUtilizationComponengNumber));
	}
	
	
	/**
	 * Set the specific component of cloudlet as disabled, the
	 * configuredUtilization is the goal utilization that would be reduced. And
	 * this function aims to find the component with highest utilization / price component
	 * and disable it.
	 *
	 * To be optimized......
	 * 
	 * @param cloudlet
	 * @param configuredUtilization
	 * @author minxianx
	 */
	public void configureOptionalComponentsByHighestUtilizationAndPriceRatio(PowerHost host, Cloudlet cloudlet, double configuredUtilization) {
		int highestUtilizationAndPriceRatioComponengNumber = 0;
		double highestUtilizationAndPriceRatio = 0.0;
		double utilizationAndPriceRatio = 0.0;
		int index = 0;
		for (CloudletOptionalComponent coc : cloudlet.getCloudletOptionalComponentList()) {
			if (isTagInSet(host.getDisabaledTagsSet(), coc.getComponentTag())) {
				coc.setEnabled(false);
			}

			utilizationAndPriceRatio = coc.getComponentUtilization() / coc.getComponentPrice();
			if (utilizationAndPriceRatio > highestUtilizationAndPriceRatio) {
				highestUtilizationAndPriceRatio = utilizationAndPriceRatio;
				highestUtilizationAndPriceRatioComponengNumber = index;
			}
			index++;
		}
		cloudlet.getCloudletOptionalComponentList().get(highestUtilizationAndPriceRatioComponengNumber).setEnabled(false);
		host.getDisabaledTagsSet().add(
				cloudlet.getCloudletOptionalComponentList().get(highestUtilizationAndPriceRatioComponengNumber).getComponentTag());

		updateHostRevenueLoss(host, cloudlet.getCloudletOptionalComponentList().get(highestUtilizationAndPriceRatioComponengNumber));
	}

	/**
	 * Set the specific component of cloudlet as disabled, the
	 * configuredUtilization is the goal utilization that would be reduced. And
	 * this function aims to find the component with lowest price component
	 * and disable it.
	 *
	 * To be optimized......
	 * 
	 * @param cloudlet
	 * @param configuredUtilization
	 * @author minxianx
	 */
	public void configureOptionalComponentsByLowestPrice(PowerHost host, Cloudlet cloudlet, double configuredUtilization) {
		int lowestPriceComponengNumber = 0;
		double lowestPrice = 1.0;
		double price = 0.0;
		int index = 0;
		for (CloudletOptionalComponent coc : cloudlet.getCloudletOptionalComponentList()) {
			if (isTagInSet(host.getDisabaledTagsSet(), coc.getComponentTag())) {
				coc.setEnabled(false);
			}

			price = coc.getComponentPrice();
			if (price < lowestPrice) {
				lowestPrice = price;
				lowestPriceComponengNumber = index;
			}
			index++;
		}
		cloudlet.getCloudletOptionalComponentList().get(lowestPriceComponengNumber).setEnabled(false);
		host.getDisabaledTagsSet().add(
				cloudlet.getCloudletOptionalComponentList().get(lowestPriceComponengNumber).getComponentTag());

		updateHostRevenueLoss(host, cloudlet.getCloudletOptionalComponentList().get(lowestPriceComponengNumber));
	}
	
	/**
	 * Update the revenue loss of host
	 * 
	 * @param host
	 * @param coc
	 * @author minxianx
	 */
	public void updateHostRevenueLoss(PowerHost host, CloudletOptionalComponent coc) {
		double hostRevenueLoss = host.getRevenueLoss() + coc.getComponentPrice();
		host.setRevenueLoss(hostRevenueLoss);

	}

	/**
	 * Update the ability (enabled or disabled) of component
	 * 
	 * @param cloudlet
	 * @author minxianx
	 */
	public void updateOptionalComponents(Cloudlet cloudlet) {
		for (CloudletOptionalComponent coc : cloudlet.getCloudletOptionalComponentList()) {
			coc.setEnabled(true);
		}
	}

	/**
	 * Get the utilization of Cloudlet after dimmer is triggered
	 * 
	 * @param cloudlet
	 * @return
	 * @author minxianx
	 */
	public double getUtilizaitonAfterDimmer(double previousUtilizationOfCpu, Cloudlet cloudlet) {
		double dimmierUtilization = 0.0;
		for (CloudletOptionalComponent coc : cloudlet.getCloudletOptionalComponentList()) {
			if (coc.isEnabled() == false) {
				dimmierUtilization += coc.getComponentUtilization();
			}
		}
		if(previousUtilizationOfCpu - dimmierUtilization <= 0){
			return DimmerConstants.DimmerComponentLowerThreshold;
		}
		return previousUtilizationOfCpu - dimmierUtilization;
	}

	/**
	 * Get the revenue loss of data center
	 * 
	 * @return
	 * @author minxianx
	 */
	public double getDataCenterRevenueLoss() {
		double dataCenterRevenueLoss = 0.0;
		for (PowerHost host : this.<PowerHost> getHostList()) {
			dataCenterRevenueLoss += host.getRevenueLoss();
		}
		return dataCenterRevenueLoss;
	}
	
	/**
	 * Check whether the tags are the set
	 * @param hashSet
	 * @param tag
	 * @author minxianx
	 * @return
	 */
	public boolean isTagInSet(Set<String> hashSet, String tag) {
		boolean tagInSet = false;
		for(String string: hashSet){
			if(string.equals(tag)){
				tagInSet = true;
			}
		}
		return tagInSet;
	}
	
	/**
	 * Get the number of how many time the dimmer has been triggered
	 * @return
	 * @author minxianx
	 */
	public int getDimmerTime(){
		
		return dimmerTimes;
	}
	
	public int getTimesMayTriggerDimmer(){
		return timeFrameMayTriggeredDimmer.size() * BrownoutConstants.NUMBER_OF_HOSTS;
	}
	
	
	/**
	 * Get the linked hash map that records the number of active hosts at each interval
	 * @return
	 * @author minxianx
	 */
	public LinkedHashMap<Double, Integer> getNumberOfActiveHostsMap(){
		return numberOfActiveHostMap;
	}
}
