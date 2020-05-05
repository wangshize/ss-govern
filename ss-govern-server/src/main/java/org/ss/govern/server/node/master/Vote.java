package org.ss.govern.server.node.master;

import org.ss.govern.core.constants.NodeRequestType;

import java.nio.ByteBuffer;

/**
 * 选票对象
 * @author wangsz
 * @create 2020-04-08
 **/
public class Vote {

    /**
     * 投票人id
     * */
    private Integer voterId;

    /**
     * 候选人id
     * */
    private Integer candidateId;

    /**
     * 投票轮次
     * */
    private Integer voteRound;

    public Vote(Integer voterId, Integer candidateId, Integer voteRound) {
        this.voterId = voterId;
        this.candidateId = candidateId;
        this.voteRound = voteRound;
    }

    public Vote(ByteBuffer message) {
        int requestType = message.getInt();
        if(NodeRequestType.VOTE == requestType) {
            this.voterId = message.getInt();
            this.candidateId = message.getInt();
            this.voteRound = message.getInt();
        }
    }

    public void nextRound() {
        voteRound++;
    }

    public ByteBuffer toRequestByteBuffer() {
        byte[] bytes = new byte[16];
        ByteBuffer msgByteBuffer = ByteBuffer.wrap(bytes);
        msgByteBuffer.clear();
        msgByteBuffer.putInt(NodeRequestType.VOTE);
        msgByteBuffer.putInt(voterId);
        msgByteBuffer.putInt(candidateId);
        msgByteBuffer.putInt(voteRound);
        return msgByteBuffer;
    }

    public Integer getVoterId() {
        return voterId;
    }

    public void setVoterId(Integer voterId) {
        this.voterId = voterId;
    }

    public Integer getCandidateId() {
        return candidateId;
    }

    public void setCandidateId(Integer candidateId) {
        this.candidateId = candidateId;
    }

    public Integer getVoteRound() {
        return voteRound;
    }

    public void setVoteRound(Integer voteRound) {
        this.voteRound = voteRound;
    }

    @Override
    public String toString() {
        return "Vote{" +
                "voterId=" + voterId +
                ", candidateId=" + candidateId +
                ", voteRound=" + voteRound +
                '}';
    }
}
