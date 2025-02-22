package top.huajieyu001.dto;

public class FaultInfo {

    private Long retention;
    private Long fix;
    private Long fault;
    private boolean softwareFault;

    public FaultInfo() {
    }

    public FaultInfo(Long retention, Long fix, Long fault) {
        this.retention = retention;
        this.fix = fix;
        this.fault = fault;
    }

    public Long getRetention() {
        return retention;
    }

    public void setRetention(Long retention) {
        this.retention = retention;
    }

    public Long getFix() {
        return fix;
    }

    public void setFix(Long fix) {
        this.fix = fix;
    }

    public Long getFault() {
        return fault;
    }

    public void setFault(Long fault) {
        this.fault = fault;
    }

    public boolean isSoftwareFault() {
        return softwareFault;
    }

    public void setSoftwareFault(boolean softwareFault) {
        this.softwareFault = softwareFault;
    }
}
