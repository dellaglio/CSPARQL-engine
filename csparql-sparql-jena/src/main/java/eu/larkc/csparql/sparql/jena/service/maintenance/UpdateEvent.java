package eu.larkc.csparql.sparql.jena.service.maintenance;

import java.util.Comparator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.sparql.engine.binding.Binding;

import eu.larkc.csparql.sparql.jena.JenaQuery;


public class UpdateEvent {
	private static Logger logger = LoggerFactory.getLogger(UpdateEvent.class);

	//public int IDToUpdateInBKG;
	public Binding BKGBindingToUpdate;
	public int changeRate;
	public long expiredEvalutionTime;
	public long creatTime; // not sure if useful
	public int remainingEva;
	public int scoreForUpdate;
	public long scheduledUpdateTime;

	public int calculateScore(int changeRate, long currentTempEvluationTime, int remainingEva) {
		long tempExpirationTimeAfterScheduledTime = this.scheduledUpdateTime + changeRate - currentTempEvluationTime;
		int validSlideAfterScheduledTime = (int) Math.ceil((double) tempExpirationTimeAfterScheduledTime
				/ (double) (QueryIterServiceMaintainedCache.getSlideLength() * 1000L));
		logger.debug("V= "+validSlideAfterScheduledTime);
		int score = Math.min(validSlideAfterScheduledTime, remainingEva);
		// score could be zero
		return score;
	}

	public UpdateEvent(Binding id, int changeRate, long currentTempEvluationTime, int remainingEva, long currentTime,long bbt) {
		this.BKGBindingToUpdate = id;
		this.creatTime = currentTime;
		this.changeRate = changeRate;
		// the evaluation time it needs update
		this.expiredEvalutionTime = currentTempEvluationTime;
		this.remainingEva = remainingEva;
		double times = Math.ceil((double) (currentTime - bbt) / (double) changeRate);
		long newbbt = bbt + changeRate * (long) times;
		newbbt -= changeRate;
		// change to use bbt
		// time is still valid range is [)
		this.scheduledUpdateTime = newbbt;
		this.scoreForUpdate = this.calculateScore(changeRate, currentTempEvluationTime, remainingEva);
	}

	public static UpdateEvent planOneUpdateEvent(Binding originalID, 
			int changeRate, long leavingWindowTime, long evaluationTime,long bbt) {
		long timeInFrontOfTheStock = leavingWindowTime - evaluationTime;
		int evaInFrontOfTheStock = (int) Math.ceil((double) timeInFrontOfTheStock / (double) (QueryIterServiceMaintainedCache.getSlideLength() * 1000L));
		logger.debug(originalID+ "L= "+evaInFrontOfTheStock);
		UpdateEvent tempEvent = new UpdateEvent(originalID, changeRate, evaluationTime, evaInFrontOfTheStock, evaluationTime,bbt);
		// score can be 0, since a data can leave the window before it expires
		// if (tempEvent.scoreForUpdate == 0 && evaluationTime !=
		// Config.INSTANCE.getQueryWindowWidth())
		// throw new RuntimeException("score cannot be 0");
		return tempEvent;
	}

	@Override
	public String toString() {
		return expiredEvalutionTime + " " + BKGBindingToUpdate + " " + scoreForUpdate;
	}

	public String toFullString() {
		return BKGBindingToUpdate + " " + expiredEvalutionTime + " " + remainingEva + " " + scoreForUpdate;
	}

	public static class Comparators {

		/*public static Comparator<UpdateEvent> ID = new Comparator<UpdateEvent>() {
			public int compare(UpdateEvent o1, UpdateEvent o2) {
				if (Integer.compare(o1.IDToUpdateInBKG, o2.IDToUpdateInBKG) != 0)
					return Integer.compare(o1.IDToUpdateInBKG, o2.IDToUpdateInBKG);
				else if (Long.compare(o1.scheduledUpdateTime, o2.scheduledUpdateTime) == 0)
					throw new RuntimeException("at one time update an data twice in SCHEDULEDTIME comparator");
				else
					return Long.compare(o1.scheduledUpdateTime, o2.scheduledUpdateTime);
			}
		};*/
		public static Comparator<UpdateEvent> SCORE = new Comparator<UpdateEvent>() {
			public int compare(UpdateEvent o1, UpdateEvent o2) {
				if (Integer.compare(o1.scoreForUpdate, o2.scoreForUpdate) != 0)
					return Integer.compare(o1.scoreForUpdate, o2.scoreForUpdate);
				else if (Long.compare(o1.scheduledUpdateTime,
						o2.scheduledUpdateTime) != 0)
					return Long.compare(o2.scheduledUpdateTime,
							o1.scheduledUpdateTime);
				else
					return Integer.compare(o1.remainingEva, o2.remainingEva);
			}
		};
		public static Comparator<UpdateEvent> SCHEDULEDTIME = new Comparator<UpdateEvent>() {
			public int compare(UpdateEvent o1, UpdateEvent o2) {
				if (Long.compare(o1.scheduledUpdateTime, o2.scheduledUpdateTime) != 0)
					return Long.compare(o1.scheduledUpdateTime, o2.scheduledUpdateTime);
				// else if (Integer.compare(o1.IDToUpdateInBKG,
				// o2.IDToUpdateInBKG) == 0) {
				// System.out.println("compare" + o1.IDToUpdateInBKG + " " +
				// o2.IDToUpdateInBKG);
				// throw new
				// RuntimeException("at one time update an data twice in SCHEDULEDTIME comparator");
				// }
				else
					return 0;//Integer.compare(o1.remainingEva, o2.remainingEva);
			}
		};
	}
}
