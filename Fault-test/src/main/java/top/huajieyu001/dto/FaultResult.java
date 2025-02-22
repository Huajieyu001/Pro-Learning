package top.huajieyu001.dto;

public class FaultResult {

    // 设备ID
    private Long deviceId;
    // 硬件故障-平均工单滞留时间
    private Long hdAvgRetention;
    // 硬件故障-最长工单滞留时间
    private Long hdMaxRetention;
    // 硬件故障-平均修复时间
    private Long hdAvgFix;
    // 硬件故障-最长故障时间
    private Long hdMaxFault;
    // 软件故障-平均工单滞留时间
    private Long sfAvgRetention;
    // 软件故障-最长工单滞留时间
    private Long sfMaxRetention;
    // 软件故障-平均修复时间
    private Long sfAvgFix;
    // 软件故障-最长故障时间
    private Long sfMaxFault;

    public FaultResult(Long deviceId, Long hdAvgRetention, Long hdMaxRetention, Long hdAvgFix, Long hdMaxFault, Long sfAvgRetention, Long sfMaxRetention, Long sfAvgFix, Long sfMaxFault) {
        this.deviceId = deviceId;
        this.hdAvgRetention = hdAvgRetention;
        this.hdMaxRetention = hdMaxRetention;
        this.hdAvgFix = hdAvgFix;
        this.hdMaxFault = hdMaxFault;
        this.sfAvgRetention = sfAvgRetention;
        this.sfMaxRetention = sfMaxRetention;
        this.sfAvgFix = sfAvgFix;
        this.sfMaxFault = sfMaxFault;
    }

    public FaultResult() {
    }

    public Long getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(Long deviceId) {
        this.deviceId = deviceId;
    }

    public Long getHdAvgRetention() {
        return hdAvgRetention;
    }

    public void setHdAvgRetention(Long hdAvgRetention) {
        this.hdAvgRetention = hdAvgRetention;
    }

    public Long getHdMaxRetention() {
        return hdMaxRetention;
    }

    public void setHdMaxRetention(Long hdMaxRetention) {
        this.hdMaxRetention = hdMaxRetention;
    }

    public Long getHdAvgFix() {
        return hdAvgFix;
    }

    public void setHdAvgFix(Long hdAvgFix) {
        this.hdAvgFix = hdAvgFix;
    }

    public Long getHdMaxFault() {
        return hdMaxFault;
    }

    public void setHdMaxFault(Long hdMaxFault) {
        this.hdMaxFault = hdMaxFault;
    }

    public Long getSfAvgRetention() {
        return sfAvgRetention;
    }

    public void setSfAvgRetention(Long sfAvgRetention) {
        this.sfAvgRetention = sfAvgRetention;
    }

    public Long getSfMaxRetention() {
        return sfMaxRetention;
    }

    public void setSfMaxRetention(Long sfMaxRetention) {
        this.sfMaxRetention = sfMaxRetention;
    }

    public Long getSfAvgFix() {
        return sfAvgFix;
    }

    public void setSfAvgFix(Long sfAvgFix) {
        this.sfAvgFix = sfAvgFix;
    }

    public Long getSfMaxFault() {
        return sfMaxFault;
    }

    public void setSfMaxFault(Long sfMaxFault) {
        this.sfMaxFault = sfMaxFault;
    }
}
