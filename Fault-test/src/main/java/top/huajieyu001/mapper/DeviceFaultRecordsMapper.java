package top.huajieyu001.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import top.huajieyu001.entity.DeviceFaultRecords;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;

import java.util.List;

/**
 * <p>
 *  Mapper 接口
 * </p>
 *
 * @author Huajieyu
 * @since 2025-02-19
 */
public interface DeviceFaultRecordsMapper extends BaseMapper<DeviceFaultRecords> {

    public List<DeviceFaultRecords> selectByStartTime2EndTime(@Param("startTime")String startTime, @Param("endTime") String endTime);

}
